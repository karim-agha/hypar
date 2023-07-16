use {
  crate::{
    channel::Channel,
    stream::SubstreamHandler,
    wire::{AddressablePeer, Message},
    Config,
  },
  libp2p::{
    core::{connection::ConnectionId, transport::ListenerId, ConnectedPoint},
    multiaddr::Protocol,
    swarm::{
      CloseConnection,
      NetworkBehaviour,
      NetworkBehaviourAction,
      NotifyHandler,
      PollParameters,
    },
    Multiaddr,
    PeerId,
  },
  std::{
    net::{Ipv4Addr, Ipv6Addr},
    task::{Context, Poll},
  },
  tracing::debug,
};

/// Represents a behaviour level event that is emitted
/// by the protocol. Events are ordered by their occurance
/// time and accessed by polling the network stream.
#[derive(Debug, Clone)]
pub(crate) enum Event {
  /// Emitted when the network discovers new public address pointing to the
  /// current node.
  LocalAddressDiscovered(Multiaddr),

  /// Emitted when a connection is created between two peers.
  ///
  /// This is emitted only once regardless of the number of HyParView
  /// overlays the two peers share. All overlapping overlays share the
  /// same connection.
  ConnectionEstablished {
    peer: AddressablePeer,
    endpoint: ConnectedPoint,
    connection_id: ConnectionId,
  },

  /// Emitted when a connection is closed between two peers.
  ///
  /// This is emitted when the last HyparView overlay between the two
  /// peers is destroyed and they have no common topics anymore. Also
  /// emitted when the connection is dropped due to transport layer failure.
  ConnectionClosed(PeerId, ConnectionId),

  /// Emitted when a message is received on the wire from a connected peer.
  MessageReceived(PeerId, ConnectionId, Message),
}

pub(crate) struct Behaviour {
  config: Config,
  events: Channel<Event>,
  disconnects: Channel<(PeerId, ConnectionId)>,
  outmsgs: Channel<(PeerId, ConnectionId, Message)>,
}

impl Behaviour {
  pub fn new(config: Config) -> Self {
    Self {
      config,
      events: Channel::new(),
      outmsgs: Channel::new(),
      disconnects: Channel::new(),
    }
  }

  pub fn send_to(&self, peer: PeerId, connection: ConnectionId, msg: Message) {
    self.outmsgs.send((peer, connection, msg));
  }

  pub fn disconnect_from(&self, peer: PeerId, connection: ConnectionId) {
    self.disconnects.send((peer, connection));
  }
}

impl NetworkBehaviour for Behaviour {
  type ConnectionHandler = SubstreamHandler;
  type OutEvent = Event;

  fn new_handler(&mut self) -> Self::ConnectionHandler {
    SubstreamHandler::new(&self.config)
  }

  fn inject_event(
    &mut self,
    peer_id: PeerId,
    connection: ConnectionId,
    event: Message,
  ) {
    debug!("injecting event from {peer_id:?} [conn {connection:?}]: {event:?}");
    self
      .events
      .send(Event::MessageReceived(peer_id, connection, event));
  }

  /// Informs the behaviour about a newly established connection to a peer.
  fn inject_connection_established(
    &mut self,
    peer_id: &PeerId,
    connection_id: &ConnectionId,
    endpoint: &ConnectedPoint,
    _: Option<&Vec<Multiaddr>>,
    _: usize,
  ) {
    self.events.send(Event::ConnectionEstablished {
      connection_id: *connection_id,
      endpoint: endpoint.clone(),
      peer: AddressablePeer {
        peer_id: *peer_id,
        addresses: [endpoint.get_remote_address().clone()]
          .into_iter()
          .collect(),
      },
    });
  }

  /// Informs the behaviour about a closed connection to a peer.
  ///
  /// A call to this method is always paired with an earlier call to
  /// [`NetworkBehaviour::inject_connection_established`] with the same peer ID,
  /// connection ID and endpoint.
  fn inject_connection_closed(
    &mut self,
    peerid: &PeerId,
    connection_id: &ConnectionId,
    _: &ConnectedPoint,
    _: SubstreamHandler,
    _: usize,
  ) {
    self
      .events
      .send(Event::ConnectionClosed(*peerid, *connection_id));
  }

  fn inject_new_listen_addr(&mut self, _: ListenerId, addr: &Multiaddr) {
    // it does not make sense to advertise localhost addresses to remote nodes
    if !is_local_address(addr) {
      self
        .events
        .send(Event::LocalAddressDiscovered(addr.clone()));
    }
  }

  fn poll(
    &mut self,
    cx: &mut Context<'_>,
    _: &mut impl PollParameters,
  ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
    // propagate any generated events to the network API.
    if let Poll::Ready(Some(event)) = self.events.poll_recv(cx) {
      return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
    }

    if let Poll::Ready(Some((peer_id, connection))) =
      self.disconnects.poll_recv(cx)
    {
      return Poll::Ready(NetworkBehaviourAction::CloseConnection {
        peer_id,
        connection: CloseConnection::One(connection),
      });
    }

    // Send next message from outbound queue by forwarding it to the
    // connection handler associated with the given peer id.
    if let Poll::Ready(Some((peer, connection, msg))) =
      self.outmsgs.poll_recv(cx)
    {
      return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
        peer_id: peer,
        handler: NotifyHandler::One(connection),
        event: msg,
      });
    }

    Poll::Pending
  }
}

/// This handles the case when the swarm api starts listening on
/// 0.0.0.0 and one of the addresses is localhost. Localhost is
/// meaningless when advertised to remote nodes, so its omitted
/// when counting local addresses
fn is_local_address(addr: &Multiaddr) -> bool {
  addr.iter().any(|p| {
    // fileter out all localhost addresses
    if let Protocol::Ip4(addr) = p {
      addr == Ipv4Addr::LOCALHOST || addr == Ipv4Addr::UNSPECIFIED
    } else if let Protocol::Ip6(addr) = p {
      addr == Ipv6Addr::LOCALHOST || addr == Ipv6Addr::UNSPECIFIED
    } else {
      false
    }
  })
}
