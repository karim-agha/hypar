use {
  crate::{
    behaviour,
    cache::ExpiringSet,
    channel::Channel,
    muxer::Muxer,
    runloop::{self, Runloop},
    topic::{self, Event, Topic},
    wire::{AddressablePeer, Message},
    Config,
  },
  futures::{Stream, StreamExt},
  libp2p::{
    core::{connection::ConnectionId, ConnectedPoint},
    identity::Keypair,
    noise::NoiseError,
    Multiaddr,
    PeerId,
    TransportError,
  },
  metrics::{gauge, increment_counter},
  multihash::Multihash,
  std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    task::{Context, Poll},
  },
  thiserror::Error,
  tokio::time::{interval, Interval},
  tracing::{debug, error, warn},
};

#[derive(Debug, Error)]
pub enum Error {
  #[error("IO Error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Transport layer security error: {0}")]
  TlsError(#[from] NoiseError),

  #[error("Transport error: {0}")]
  TransportError(#[from] TransportError<std::io::Error>),

  #[error("Topic {0} already joined")]
  TopicAlreadyJoined(String),
}

/// Commands sent by different components to the network layer
/// instructing it to perform some operation on its network-managed
/// threads and runloops. Examples of types invoking those commands
/// are Topics, Behaviour, etc.
#[derive(Debug, Clone)]
pub(crate) enum Command {
  /// Invoked by topics when adding new peer to the active view.
  Connect { addr: Multiaddr, topic: String },

  /// Invoked by topics when removing peers from the active view.
  Disconnect {
    peer: PeerId,
    connection: ConnectionId,
  },

  /// Sends a message to one peer in the active view of
  /// one of the topics.
  SendMessage {
    peer: PeerId,
    connection: ConnectionId,
    msg: Message,
  },

  /// Immediately disconnects a peer from all topics
  /// and forbids it from connecting again to this node.
  ///
  /// This is invoked on protocol violation.
  BanPeer(PeerId),

  /// Invoked by the runloop when a behaviour-level event
  /// is emitted on the background network thread.
  InjectEvent(behaviour::Event),
}

/// This type is the entrypoint to using the network API.
///
/// It is used to configure general network settings such as
/// the underlying transport protocol, encryption scheme, dns
/// lookup and other non-topic specific configuration values.
///
/// An instance of this type is used to join topics and acquire
/// instances of types for interacting with individul topics.
///
/// On the implementation level, this type acts as a multiplexer
/// for topics, routing incoming packets to their appropriate topic
/// instance.
pub struct Network {
  /// Global network-level configuration
  config: Config,

  /// Local identity of the current local node and all its known addresses.
  this: AddressablePeer,

  /// Network background runloop.
  ///
  /// Stored here so users of this type can block on
  /// the network object.
  runloop: Runloop,

  /// Commands sent to the network module by the runloop and topics.
  commands: Channel<Command>,

  /// All joined topic addressed by their topic name.
  /// Each topic is its own instance of HyParView overlay.
  topics: HashMap<String, Topic>,

  /// Used to track pending and active connections to peers
  /// and refcount them by the number of topics using a connection.
  muxer: Muxer,

  /// If message deduplication is turned on in config, this struct will
  /// store recent messages that were received by this node to ignore
  /// duplicates for some time.
  history: Option<ExpiringSet<Multihash>>,

  /// Last time a network tick was triggered.
  ///
  /// See Config::tick_interval for more info.
  tick: Interval,
}

impl Default for Network {
  fn default() -> Self {
    Self::new(Config::default(), Keypair::generate_ed25519())
      .expect("Failed to instantiate network instance using default config")
  }
}

impl Network {
  /// Instanciates a network object.
  pub fn new(config: Config, keypair: Keypair) -> Result<Self, Error> {
    let peer_id = keypair.public().into();
    debug!("local identity: {peer_id}");
    let commands = Channel::new();
    Ok(Self {
      topics: HashMap::new(),
      muxer: Muxer::new(&config),
      tick: interval(config.maintenance_tick_interval),
      history: config.dedupe_interval.map(ExpiringSet::new),
      runloop: Runloop::new(&config, keypair, commands.sender())?,
      this: AddressablePeer {
        peer_id,
        addresses: HashSet::new(), // none discovered yet
      },
      config,
      commands,
    })
  }

  /// Joins a new topic on this network.
  ///
  /// The config value specifies mainly the topic name and
  /// a list of bootstrap peers. If the bootstrap list is empty
  /// then this node will not dial into any peers but listen on
  /// incoming connections on that topic. It will not receive or
  /// send any values unless at least one other node connects to it.
  pub fn join(&mut self, config: topic::Config) -> Result<Topic, Error> {
    if self.topics.contains_key(&config.name) {
      return Err(Error::TopicAlreadyJoined(config.name));
    }

    let name = config.name.clone();
    self.topics.insert(
      name.clone(),
      Topic::new(
        config,
        self.config.clone(),
        self.this.clone(),
        self.commands.sender(),
      ),
    );

    increment_counter!("topics_joined");
    Ok(self.topics.get(&name).unwrap().clone())
  }

  /// Runs the network event loop.
  ///
  /// This loop must be running all the time to drive the network layer,
  /// this function makes it easy to move the whole network layer to the
  /// background by calling:
  ///
  /// ```rust
  /// tokio::spawn(network.runloop());
  /// ```
  ///
  /// The network object is needed only to join topics, after that all
  /// interactions with the network happen through the [`Topic`] instances
  /// created when calling [`Network::join`].
  pub async fn runloop(mut self) {
    while let Some(()) = self.next().await {
      // metrics & observability
      gauge!("connected_peers", self.muxer.assigned_count() as f64);
      gauge!("pending_peers", self.muxer.unassigned_count() as f64);
    }
  }
}

impl Network {
  fn ban_peer(&self, peer: PeerId) {
    increment_counter!("peers_banned");
    warn!("Banning peer {peer}");
    self.runloop.send_command(runloop::Command::BanPeer(peer));
  }

  /// Invoked by topics when they are attempting to establish
  /// a new active connection with some peer who's identity is
  /// not known yet but we know its address.
  fn begin_connect(&mut self, addr: Multiaddr, topic: String) {
    self.muxer.put_dial(addr.clone(), topic.clone());
    if self.muxer.next_dial(&addr, &topic) {
      debug!(?addr, ?topic, "next_dial");
      self.runloop.send_command(runloop::Command::Connect(addr));
    }
  }

  /// Invoked by the background network runloop when a connection
  /// is established with a peer and its identity is known.
  fn complete_connect(
    &mut self,
    peer: AddressablePeer,
    connection: ConnectionId,
    endpoint: ConnectedPoint,
  ) {
    if endpoint.is_dialer() {
      // local node initiated the connection, we know which topic(s)
      // called into this peer and we can resolve it right away.
      increment_counter!("dials_outbound");
      if let Some(topic) = self.muxer.match_dial(&peer, connection) {
        for addr in &peer.addresses {
          if self.muxer.next_dial(addr, &topic) {
            debug!(?addr, ?topic, "next_dial");
            self
              .runloop
              .send_command(runloop::Command::Connect(addr.clone()));
            break;
          }
        }
        let topic = self.topics.get_mut(&topic).unwrap_or_else(|| {
          panic!("bug: dialed a peer on a topic not joined by local node");
        });
        topic.inject_event(Event::PeerConnected(peer.clone(), connection));
      } else {
        unreachable!("bug: a dialed address is not tracked by the muxer");
      }
    } else {
      // remote peer initiated the connection, we still don't know
      // which topic is used for this connection, the first message
      // received on this connection will reveal the topic.
      increment_counter!("dials_inbound");
      self.muxer.register(peer, connection)
    }
  }

  /// invoked when this node initiated a disconnect from a remote peer
  fn begin_disconnect(&mut self, peer: PeerId, connection: ConnectionId) {
    self
      .runloop
      .send_command(runloop::Command::Disconnect(peer, connection));
  }

  /// This happens when the last topic on this node requests a connection to a
  /// peer to be closed, or the remote peer abruptly closes the TCP link.
  fn complete_disconnect(&mut self, peer: PeerId, connection: ConnectionId) {
    if let Some(topic) = self.muxer.resolve_topic(&peer, &connection) {
      let topic = self
        .topics
        .get_mut(topic)
        .expect("resolve would return None");
      topic.inject_event(Event::PeerDisconnected(peer, connection));
    } else {
      debug!(?peer, "disconnected from unknown topic");
    }

    self.muxer.disconnect(peer, connection);
  }

  fn append_local_address(&mut self, address: Multiaddr) {
    self.this.addresses.insert(address.clone());

    // update all topics about new local addresses
    for topic in self.topics.values_mut() {
      topic.inject_event(topic::Event::LocalAddressDiscovered(address.clone()));
    }
  }

  fn accept_message(
    &mut self,
    from: PeerId,
    connection: ConnectionId,
    msg: Message,
  ) {
    increment_counter!(
      "messages_received",
      "peer" => from.to_base58(),
      "topic" => msg.topic.clone()
    );

    let topic = match self.topics.get_mut(&msg.topic) {
      Some(topic) => topic,
      None => {
        self // A peer is sending message on an unrecognized topic. Disconnect.
          .runloop
          .send_command(runloop::Command::Disconnect(from, connection));
        return;
      }
    };

    // if deduplication is enabled and we've seen this message
    // recently, then ignore it and don't propagate to topics.
    if let Some(ref mut history) = self.history {
      if history.insert(*msg.hash()) {
        increment_counter!(
          "duplicate_messages",
          "peer" => from.to_base58(),
          "topic" => msg.topic.clone()
        );
        return;
      }
    }

    // if first message on this topic from this peer:
    if let Some(peer) = self.muxer.assign(from, connection, &msg.topic) {
      // this is the first message to a topic on this connection,
      // it was successfully assigned, and we know the
      // peer -> connectionid -> topic mapping.
      // signal to the topic that a peer was connected before
      // routing any messages
      topic.inject_event(topic::Event::PeerConnected(peer, connection));
    }

    let topic_name = self
      .muxer
      .resolve_topic(&from, &connection)
      .expect("The connection ID should be mapped by now");

    if *topic_name == msg.topic {
      topic.inject_event(topic::Event::MessageReceived(from, msg, connection));
    } else {
      // seems like this sender started sending messages with a different
      // topic name on the same connection. This is a protocol violation.
      self.ban_peer(from);
    }
  }

  /// Processes events generated by the background runloop
  fn inject_event(&mut self, event: behaviour::Event) {
    debug!("network event: {event:?}");
    match event {
      behaviour::Event::MessageReceived(from, conn, msg) => {
        increment_counter!(
          "received_messages",
          "peer" => from.to_base58(),
          "topic" => msg.topic.clone()
        );
        self.accept_message(from, conn, msg)
      }
      behaviour::Event::LocalAddressDiscovered(addr) => {
        increment_counter!("local_address_discovered");
        self.append_local_address(addr)
      }
      behaviour::Event::ConnectionEstablished {
        peer,
        endpoint,
        connection_id,
      } => {
        increment_counter!(
          "connections_established", 
          "peer" => peer.peer_id.to_base58());
        self.complete_connect(peer, connection_id, endpoint)
      }
      behaviour::Event::ConnectionClosed(peer, connection_id) => {
        increment_counter!(
          "connections_closed",
          "peer" => peer.to_base58()
        );
        self.complete_disconnect(peer, connection_id);
      }
    }
  }
}

/// Drives the network event loop.
///
/// This should not be used directly, use [`Self::runloop`] instead.
impl Stream for Network {
  type Item = ();

  fn poll_next(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<Self::Item>> {
    if self.tick.poll_tick(cx).is_ready() {
      for topic in self.topics.values_mut() {
        topic.inject_event(Event::Tick);
      }

      // prune all expiring collections
      self.muxer.prune_expired();

      if let Some(ref mut history) = self.history {
        history.prune_expired();
      }

      // check outstanding requested dials
      if let Some((topic, addr)) = self.muxer.poll_dial() {
        if self.muxer.next_dial(&addr, &topic) {
          debug!(?addr, ?topic, "next_dial");
          self.runloop.send_command(runloop::Command::Connect(addr));
        }
      }

      return Poll::Ready(Some(()));
    }

    if let Poll::Ready(Some(command)) = self.commands.poll_recv(cx) {
      debug!("network command: {command:?}");
      match command {
        Command::Connect { addr, topic } => {
          self.begin_connect(addr, topic);
        }
        Command::Disconnect { peer, connection } => {
          self.begin_disconnect(peer, connection)
        }
        Command::SendMessage {
          peer,
          msg,
          connection,
        } => {
          self.runloop.send_command(runloop::Command::SendMessage {
            peer,
            connection,
            msg,
          });
        }
        Command::BanPeer(peer) => self.ban_peer(peer),
        Command::InjectEvent(event) => self.inject_event(event),
      }

      return Poll::Ready(Some(()));
    }

    Poll::Pending
  }
}
