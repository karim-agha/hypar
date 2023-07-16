//! Connections in HyparView are long-lived between active nodes.
//! Types in this module implement an asynchronous state machine
//! for handling inbound and outbound network frames.
//!
//! A single connection with a peer may communicate on many
//! different libp2p protocols simultaneously. Network packets
//! are multiplexed with a transport muxer. Here we deal only with
//! substreams that belong to the HyparView protocol on the same
//! connection.

use {
  crate::{
    channel::Channel,
    codec::Codec,
    upgrade::{self, ProtocolUpgrade},
    wire,
    Config,
  },
  asynchronous_codec::Framed,
  futures::{Sink, StreamExt},
  libp2p::{
    core::{muxing::SubstreamBox, Negotiated},
    swarm::{
      ConnectionHandler,
      ConnectionHandlerEvent,
      ConnectionHandlerUpgrErr,
      KeepAlive,
      NegotiatedSubstream,
      SubstreamProtocol,
    },
  },
  std::{
    io,
    pin::Pin,
    task::{Context, Poll},
  },
  tracing::{error, warn},
};

/// State of the protocol inbound async reader
enum InboundSubstreamState {
  /// Waiting for a message from the remote peer. This is the idle state for an
  /// inbound stream.
  AwaitingRead(Framed<NegotiatedSubstream, Codec>),

  /// Substream is closing gracefully.
  Closing(Framed<NegotiatedSubstream, Codec>),

  /// Substream connection terminated abruptly
  Poisoned,
}

/// State of the protocol outbound async writer
enum OutboundSubstreamState {
  /// Protocol upgrade requested and waiting for the protocol upgrade
  /// negotiation to complete.
  SubstreamRequested,

  /// Waiting for the local node to send a message to the remote peer.
  /// This is the idle state for an outbound substream after both
  /// peers negotiated and settled on the used protocol.
  AwaitingWrite(Framed<NegotiatedSubstream, Codec>),

  /// Awaiting an async write on the stream to complete.
  PendingWrite(Framed<NegotiatedSubstream, Codec>, wire::Message),

  /// Awaiting an async flush on the stream to complete.
  PendingFlush(Framed<NegotiatedSubstream, Codec>),

  /// The substream is being closed. Used by either substream.
  Closing(Framed<NegotiatedSubstream, Codec>),

  /// Substream connection terminated abruptly
  Poisoned,
}

/// Handles and manages a single long-lived substream connection with a peer in
/// the active view.
pub struct SubstreamHandler {
  /// Protocol upgrade for hyparview
  listen_protocol: SubstreamProtocol<ProtocolUpgrade, ()>,

  /// A single long-lived inbound substream state machine
  inbound_stream: Option<InboundSubstreamState>,

  /// A single long-lived outbound substream state machine
  outbound_stream: Option<OutboundSubstreamState>,

  /// By default this is set to `KeepAlive::Yes`.
  /// When we want to terminate this connection gracefully then this
  /// value is set to `KeepAlive::No`, and the protocol will close
  /// the connection on next tick.
  keep_alive: KeepAlive,

  /// List of messaged scheduled to be writted on this substream.
  outbound_messages: Channel<wire::Message>,
}

type SubstreamHandlerEvent = ConnectionHandlerEvent<
  <SubstreamHandler as ConnectionHandler>::OutboundProtocol,
  <SubstreamHandler as ConnectionHandler>::OutboundOpenInfo,
  <SubstreamHandler as ConnectionHandler>::OutEvent,
  <SubstreamHandler as ConnectionHandler>::Error,
>;

impl SubstreamHandler {
  pub fn new(config: &Config) -> Self {
    Self {
      listen_protocol: SubstreamProtocol::new(
        ProtocolUpgrade::new(config.max_transmit_size),
        (),
      ),
      inbound_stream: None,
      outbound_stream: None,
      keep_alive: KeepAlive::Yes,
      outbound_messages: Channel::new(),
    }
  }
}

impl ConnectionHandler for SubstreamHandler {
  type Error = upgrade::Error;
  type InEvent = wire::Message;
  type InboundOpenInfo = ();
  type InboundProtocol = upgrade::ProtocolUpgrade;
  type OutEvent = wire::Message;
  type OutboundOpenInfo = ();
  type OutboundProtocol = upgrade::ProtocolUpgrade;

  /// Returns an object that negotiates protocol upgrade to this gossip
  /// protocol with a remote peer.
  fn listen_protocol(
    &self,
  ) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
    self.listen_protocol.clone()
  }

  /// Protocol negotiated successfully with the remote peer for inbound frames.
  /// The `substream` objest is an async reader over a muxer.
  fn inject_fully_negotiated_inbound(
    &mut self,
    substream: Framed<Negotiated<SubstreamBox>, Codec>,
    _: Self::InboundOpenInfo,
  ) {
    // we're ready to start receiving frames from the remote peer over this
    // stream. Put the inbound state machine in idle state and store the
    // stream object.
    self.inbound_stream = Some(InboundSubstreamState::AwaitingRead(substream))
  }

  /// Protocol negotiated successfully with the remote peer for outbound frames.
  fn inject_fully_negotiated_outbound(
    &mut self,
    substream: Framed<Negotiated<SubstreamBox>, Codec>,
    _: Self::OutboundOpenInfo,
  ) {
    // we're ready to start sending frames to the remote peer over this stream.
    // Put the outbound state machine in idle state and store the stream object.
    self.outbound_stream =
      Some(OutboundSubstreamState::AwaitingWrite(substream));
  }

  /// Called by libp2p whenever a message is sent over this substream to
  /// the peer by upper levels of the stack.
  fn inject_event(&mut self, event: Self::InEvent) {
    // store it in the output queue. When users of this
    // object will poll for events they will get it in FIFO order.
    self.outbound_messages.send(event);
  }

  /// Failed to negotiate protocol upgrade. Invalidate substream.
  fn inject_dial_upgrade_error(
    &mut self,
    _: Self::OutboundOpenInfo,
    error: ConnectionHandlerUpgrErr<upgrade::Error>,
  ) {
    warn!("Dial upgrade error: {error:?}");
    self.inbound_stream = Some(InboundSubstreamState::Poisoned);
    self.outbound_stream = Some(OutboundSubstreamState::Poisoned);
  }

  /// Polled by libp2p to check if this substream should still be open.
  /// When set to `KeepAliva::No`, the substream will be terminated gracefully.
  fn connection_keep_alive(&self) -> KeepAlive {
    self.keep_alive
  }

  /// Invoked by the libp2p event loop
  fn poll(
    &mut self,
    cx: &mut Context<'_>,
  ) -> Poll<
    ConnectionHandlerEvent<
      Self::OutboundProtocol,
      Self::OutboundOpenInfo,
      Self::OutEvent,
      Self::Error,
    >,
  > {
    // first process inbound substream events
    let inbound_poll = self.process_inbound_poll(cx);
    if !matches!(inbound_poll, Poll::<SubstreamHandlerEvent>::Pending) {
      return inbound_poll;
    }

    // then process outbound substream events
    let outbound_poll = self.process_outbound_poll(cx);
    if !matches!(outbound_poll, Poll::<SubstreamHandlerEvent>::Pending) {
      return outbound_poll;
    }

    // nothing to communicate to the caller yet for this substream
    Poll::Pending
  }
}

impl SubstreamHandler {
  /// State machine for inbound substream async reads
  fn process_inbound_poll(
    &mut self,
    cx: &mut Context<'_>,
  ) -> Poll<SubstreamHandlerEvent> {
    loop {
      match std::mem::replace(
        &mut self.inbound_stream,
        Some(InboundSubstreamState::Poisoned),
      ) {
        Some(InboundSubstreamState::AwaitingRead(mut substream)) => {
          match substream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(message))) => {
              self.inbound_stream =
                Some(InboundSubstreamState::AwaitingRead(substream));
              return Poll::Ready(ConnectionHandlerEvent::Custom(message));
            }
            Poll::Ready(Some(Err(error))) => {
              warn!("inbound stream error: {:?}", error);
            }
            Poll::Ready(None) => {
              warn!("Peer closed their outbound stream");
              self.inbound_stream =
                Some(InboundSubstreamState::Closing(substream));
            }
            Poll::Pending => {
              self.inbound_stream =
                Some(InboundSubstreamState::AwaitingRead(substream));
              break;
            }
          }
        }
        Some(InboundSubstreamState::Closing(mut substream)) => {
          match Sink::poll_close(Pin::new(&mut substream), cx) {
            Poll::Ready(res) => {
              if let Err(e) = res {
                // Don't close the connection but just drop the inbound
                // substream. In case the remote has more to
                // send, they will open up a new substream.
                warn!("Inbound substream error while closing: {:?}", e);
              }
              self.inbound_stream = None;
              if self.outbound_stream.is_none() {
                self.keep_alive = KeepAlive::No;
              }
              break;
            }
            Poll::Pending => {
              self.inbound_stream =
                Some(InboundSubstreamState::Closing(substream));
              break;
            }
          }
        }
        Some(InboundSubstreamState::Poisoned) => {
          error!("Error occurred during inbound stream processing");
          self.keep_alive = KeepAlive::No;
          break;
        }
        None => {
          self.inbound_stream = None;
          break;
        }
      }
    }
    Poll::Pending
  }

  /// State machine for outbound substream async writes
  fn process_outbound_poll(
    &mut self,
    cx: &mut Context<'_>,
  ) -> Poll<SubstreamHandlerEvent> {
    loop {
      match std::mem::replace(
        &mut self.outbound_stream,
        Some(OutboundSubstreamState::Poisoned),
      ) {
        Some(OutboundSubstreamState::AwaitingWrite(substream)) => {
          if let Poll::Ready(Some(msg)) = self.outbound_messages.poll_recv(cx) {
            self.outbound_stream =
              Some(OutboundSubstreamState::PendingWrite(substream, msg));
          } else {
            self.outbound_stream =
              Some(OutboundSubstreamState::AwaitingWrite(substream));
            break;
          }
        }
        Some(OutboundSubstreamState::PendingWrite(mut substream, message)) => {
          match Sink::poll_ready(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => {
              match Sink::start_send(Pin::new(&mut substream), message) {
                Ok(()) => {
                  self.outbound_stream =
                    Some(OutboundSubstreamState::PendingFlush(substream));
                }
                Err(upgrade::Error::MaxTransmissionSizeExceeded) => {
                  error!(
                    "Message exceeds the maximum transmission size and was \
                     dropped."
                  );
                  self.outbound_stream =
                    Some(OutboundSubstreamState::AwaitingWrite(substream));
                }
                Err(e) => {
                  error!("Error sending message: {}", e);
                  self.outbound_stream =
                    Some(OutboundSubstreamState::Closing(substream));
                  return Poll::Ready(ConnectionHandlerEvent::Close(e));
                }
              }
            }
            Poll::Ready(Err(e)) => {
              error!("outbound substream error while sending message: {:?}", e);
              self.outbound_stream =
                Some(OutboundSubstreamState::Closing(substream));
              return Poll::Ready(ConnectionHandlerEvent::Close(e));
            }
            Poll::Pending => {
              self.keep_alive = KeepAlive::Yes;
              self.outbound_stream =
                Some(OutboundSubstreamState::PendingWrite(substream, message));
              break;
            }
          }
        }
        Some(OutboundSubstreamState::PendingFlush(mut substream)) => {
          match Sink::poll_flush(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => {
              self.outbound_stream =
                Some(OutboundSubstreamState::AwaitingWrite(substream));
            }
            Poll::Ready(Err(e)) => {
              self.outbound_stream =
                Some(OutboundSubstreamState::Closing(substream));
              return Poll::Ready(ConnectionHandlerEvent::Close(e));
            }
            Poll::Pending => {
              self.keep_alive = KeepAlive::Yes;
              self.outbound_stream =
                Some(OutboundSubstreamState::PendingFlush(substream));
              break;
            }
          }
        }
        Some(OutboundSubstreamState::Closing(mut substream)) => {
          match Sink::poll_close(Pin::new(&mut substream), cx) {
            Poll::Ready(Ok(())) => {
              self.outbound_stream = None;
              if self.inbound_stream.is_none() {
                self.keep_alive = KeepAlive::No;
              }
              break;
            }
            Poll::Ready(Err(e)) => {
              warn!("Outbound substream error while closing: {:?}", e);
              return Poll::Ready(ConnectionHandlerEvent::Close(
                io::Error::new(
                  io::ErrorKind::BrokenPipe,
                  "Failed to close outbound substream",
                )
                .into(),
              ));
            }
            Poll::Pending => {
              self.keep_alive = KeepAlive::No;
              self.outbound_stream =
                Some(OutboundSubstreamState::Closing(substream));
              break;
            }
          }
        }
        Some(OutboundSubstreamState::SubstreamRequested) => {
          self.outbound_stream =
            Some(OutboundSubstreamState::SubstreamRequested);
          break;
        }
        Some(OutboundSubstreamState::Poisoned) => {
          warn!("Substream poisoned, closing connection");
          self.keep_alive = KeepAlive::No;
          break;
        }
        None => {
          self.outbound_stream =
            Some(OutboundSubstreamState::SubstreamRequested);
          return Poll::Ready(
            ConnectionHandlerEvent::OutboundSubstreamRequest {
              protocol: self.listen_protocol.clone(),
            },
          );
        }
      }
    }

    Poll::Pending
  }
}
