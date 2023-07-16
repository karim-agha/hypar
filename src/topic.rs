//! HyParView: a membership protocol for reliable gossip-based broadcast
//! Leitão, João & Pereira, José & Rodrigues, Luís. (2007). 419-429.
//! 10.1109/DSN.2007.56.

use {
  crate::{
    cache::{ExpiringMap, ExpiringSet},
    channel::Channel,
    muxer::IpAddress,
    network::Command,
    wire::{
      Action,
      AddressablePeer,
      ForwardJoin,
      Join,
      Message,
      Neighbour,
      Shuffle,
      ShuffleReply,
    },
  },
  bytes::Bytes,
  futures::Stream,
  libp2p::{core::connection::ConnectionId, Multiaddr, PeerId},
  metrics::{gauge, increment_counter},
  parking_lot::RwLock,
  rand::{
    distributions::Standard,
    rngs::StdRng,
    seq::IteratorRandom,
    thread_rng,
    Rng,
    SeedableRng,
  },
  std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
  },
  thiserror::Error,
  tokio::sync::mpsc::UnboundedSender,
  tracing::{debug, error, warn},
};

#[derive(Debug, Error)]
pub enum Error {
  #[error("Message size exceeds maximum allowed size")]
  MessageTooLarge,

  #[error("No peers connected")]
  NoConnectedPeers,
}

#[derive(Debug)]
pub struct Config {
  pub name: String,
  pub bootstrap: HashSet<Multiaddr>,
}

#[derive(Debug)]
pub enum Event {
  MessageReceived(PeerId, Message, ConnectionId),
  LocalAddressDiscovered(Multiaddr),
  PeerConnected(AddressablePeer, ConnectionId),
  PeerDisconnected(PeerId, ConnectionId),
  Tick,
}

/// Here the topic implementation lives. It is in an internal
/// struct because the public interface must be Send + Sync so
/// it can be moved across different threads. Access to this
/// type is protected by an RW lock on the outer public type.
struct TopicInner {
  /// Topic specific config.s
  topic_config: Config,

  /// Network wide config.
  network_config: crate::Config,

  /// When the last time a shuffle operation happened on this topic
  ///
  /// This also includes attemtps to shuffle that resulted in not
  /// performing the operation due to Config::shuffle_probability.
  last_shuffle: Instant,

  /// Cryptographic identity of the current node and all known TCP
  /// addresses by which it can be reached. This list of addreses
  /// is updated everytime the network layer discovers a new one.
  this_node: AddressablePeer,

  /// Events emitted to listeners on new messages received on this topic.
  outmsgs: Channel<Vec<u8>>,

  /// Commands to the network layer
  cmdtx: UnboundedSender<Command>,

  /// The active views of all nodes create an overlay that is used for message
  /// dissemination. Links in the overlay are symmetric, this means that each
  /// node keeps an open TCP connection to every other node in its active
  /// view.
  ///
  /// The active view is maintained using a reactive strategy, meaning nodes
  /// are remove when they fail.
  active_peers: HashMap<PeerId, (ConnectionId, HashSet<Multiaddr>)>,

  /// The goal of the passive view is to maintain a list of nodes that can be
  /// used to replace failed members of the active view. The passive view is
  /// not used for message dissemination.
  ///
  /// The passive view is maintained using a cyclic strategy. Periodically,
  /// each node performs shuffle operation with one of its neighbors in order
  /// to update its passive view.
  passive_peers: HashMap<PeerId, HashSet<Multiaddr>>,

  /// Peers that we have dialed by address only without knowing their
  /// identity. This is used to send JOIN messages once a connection
  /// is established.
  pending_dials: ExpiringSet<IpAddress>,

  /// Peers that we have dialed because we want to add them to the active
  /// peers. Peers in this collection will be sent NEIGHBOUR message when
  /// a connection to them is established.
  pending_neighbours: ExpiringSet<PeerId>,

  /// Peers that have received or sent a JOIN message to us.
  ///
  /// This is to prevent spamming the same peer multiple times
  /// with JOIN requests if they add us to their active view.
  ///
  /// This also prevents a peer from initiating many forward join
  /// floods on the network. Peers added to this set are never
  /// removed, instead, they expire (on both sides of the wire).
  pending_joins: ExpiringSet<PeerId>,

  /// Peers that have originated a SHUFFLE that are not in
  /// the current active view.
  ///
  /// When replying to the shuffle, a new temporary connection
  /// is established with the originator, the shuffle reply is
  /// sent and then immediately the connection is closed.
  pending_shuffle_replies: ExpiringMap<PeerId, ShuffleReply>,

  /// Peers that we've sent DISCONNECT messages and are waiting
  /// for them to close the connection with us.
  pending_disconnects: ExpiringSet<PeerId>,
}

/// A topic represents an instance of HyparView p2p overlay.
/// This type is cheap to copy and can be safely moved across
/// different threads for example to listen on topic messages
/// on a background thread.
#[derive(Clone)]
pub struct Topic {
  inner: Arc<RwLock<TopicInner>>,
}

// Public API
impl Topic {
  /// Propagate a message to connected active peers
  pub fn gossip(&self, data: Vec<u8>) -> Result<(), Error> {
    let inner = self.inner.read();
    if data.len() > inner.network_config.max_transmit_size {
      return Err(Error::MessageTooLarge);
    }

    if inner.active_peers.is_empty() {
      return Err(Error::NoConnectedPeers);
    }

    let data: Bytes = data.into();
    for (peer, (connection, _)) in inner.active_peers.iter() {
      inner.send_message(
        *peer,
        *connection,
        Message::new(
          inner.topic_config.name.clone(),
          Action::Gossip(data.clone()),
        ),
      );
    }

    Ok(())
  }
}

// internal api
impl Topic {
  pub(crate) fn new(
    topic_config: Config,
    network_config: crate::Config,
    this_node: AddressablePeer,
    cmdtx: UnboundedSender<Command>,
  ) -> Self {
    let timeout = network_config.pending_timeout;
    let mut pending_dials = ExpiringSet::<IpAddress>::new(timeout);

    // dial all bootstrap nodes
    for addr in topic_config.bootstrap.iter() {
      cmdtx
        .send(Command::Connect {
          addr: addr.clone(),
          topic: topic_config.name.clone(),
        })
        .expect("lifetime of network should be longer than topic");

      if let Ok(addr) = addr.clone().try_into() {
        pending_dials.insert(addr);
      }
    }

    Self {
      inner: Arc::new(RwLock::new(TopicInner {
        outmsgs: Channel::new(),
        last_shuffle: Instant::now(),
        active_peers: HashMap::new(),
        passive_peers: HashMap::new(),
        pending_joins: ExpiringSet::new(timeout),
        pending_neighbours: ExpiringSet::new(timeout),
        pending_disconnects: ExpiringSet::new(timeout),
        pending_shuffle_replies: ExpiringMap::new(timeout),
        cmdtx,
        this_node,
        topic_config,
        pending_dials,
        network_config,
      })),
    }
  }

  /// Called when the network layer has a new event for this topic
  pub(crate) fn inject_event(&mut self, event: Event) {
    let mut inner = self.inner.write();

    if !matches!(event, Event::Tick) {
      debug!("{}: {event:?}", inner.topic_config.name);
    }

    match event {
      Event::LocalAddressDiscovered(addr) => {
        inner.handle_new_local_address(addr);
      }
      Event::PeerConnected(peer, connection) => {
        inner.handle_peer_connected(peer, connection);
      }
      Event::PeerDisconnected(peer, connection) => {
        inner.handle_peer_disconnected(peer, connection);
      }
      Event::MessageReceived(peer, msg, connection) => {
        inner.handle_message_received(peer, msg, connection);
      }
      Event::Tick => inner.handle_tick(),
    }
  }
}

/// Event handlers
impl TopicInner {
  fn handle_new_local_address(&mut self, addr: Multiaddr) {
    self.this_node.addresses.insert(addr);
  }

  fn handle_message_received(
    &mut self,
    sender: PeerId,
    msg: Message,
    connection: ConnectionId,
  ) {
    if msg.topic != self.topic_config.name {
      panic!("{}: invalid topic in msg {:?}", self.topic_config.name, msg);
    }

    // message handlers that take a connection parameter,
    // are messages that are expected to arrive from
    // non-active peers.

    match msg.action {
      Action::Join(join) => self.consume_join(sender, join, connection),
      Action::ForwardJoin(fj) => self.consume_forward_join(sender, fj),
      Action::Neighbour(n) => self.consume_neighbor(sender, n, connection),
      Action::Shuffle(s) => self.consume_shuffle(sender, s),
      Action::ShuffleReply(sr) => {
        self.consume_shuffle_reply(sender, sr, connection)
      }
      Action::Gossip(b) => self.consume_gossip(sender, b, connection),
    }
  }

  /// This gets invoked after a connection is established with a remote peer
  /// that was dialed specifically to send them NEIGHBOUR message and form an
  /// active connection with them.
  ///
  /// Peers in this collection will not be sent JOIN or any other messages that
  /// are sent to newly connected peers.
  fn handle_pending_neighbours(
    &mut self,
    peer: &AddressablePeer,
    connection: ConnectionId,
  ) {
    if self.pending_neighbours.remove(&peer.peer_id) {
      self.send_message(
        peer.peer_id,
        connection,
        Message::new(
          self.topic_config.name.clone(),
          Action::Neighbour(Neighbour {
            peer: self.this_node.clone(),
            high_priority: self.active_peers.is_empty(),
          }),
        ),
      );
      self.add_to_active_view(peer.clone(), connection);
    }
  }

  /// This gets invoked after a connection is established with a remote peer
  /// that was dialed specifically to send them SHUFFLE message after a shuffle
  /// initiated by them was forwarded to the current node.
  ///
  /// Peers in this collection will not be sent JOIN or any other messages that
  /// are sent to newly connected peers. After respnding to the shuffle, the
  /// connection gets closed (unless they were already part of the active
  /// set on the current node).
  fn handle_pending_shuffle_replies(
    &mut self,
    peer: &AddressablePeer,
    connection: ConnectionId,
  ) {
    if let Some(reply) = self.pending_shuffle_replies.remove(&peer.peer_id) {
      // this peer originated a shuffle operation and this
      // is a temporary connection connection just to reply
      // with our unique peer info.
      self.send_message(
        peer.peer_id,
        connection,
        Message::new(
          self.topic_config.name.clone(),
          Action::ShuffleReply(reply),
        ),
      );

      increment_counter!("shuffle_reply");

      // send & close connection if not in active view
      if !self.is_active(&peer.peer_id) {
        self.disconnect(peer.peer_id, connection, "inactive shuffle reply");
      }
    }
  }

  /// If we have dialed a peer and that peer was not dialed
  /// specifically for a known operation, then send them a
  /// request to join the topic. This case occurs only when
  /// dialing a bootstrap node.
  fn handle_non_pending_connects(
    &mut self,
    peer: &AddressablePeer,
    connection: ConnectionId,
  ) {
    if !self.active_view_full()
      && self.is_pending_dial(peer)
      && !self.pending_joins.contains(&peer.peer_id)
      && !self.pending_neighbours.contains(&peer.peer_id)
      && !self.pending_shuffle_replies.contains_key(&peer.peer_id)
    {
      self.send_message(
        peer.peer_id,
        connection,
        Message::new(
          self.topic_config.name.clone(),
          Action::Join(Join {
            node: self.this_node.clone(),
          }),
        ),
      );

      // take note that we have sent a JOIN request to this node.
      // This value will expire after some time, and we will be able
      // to retry joining if no peer at all establishes a connection
      // with us.
      self.pending_joins.insert(peer.peer_id);
    }
  }

  /// Invoked when a connection is established with a remote peer.
  /// When a node is dialed, we don't know its identity, only the
  /// address we dialed it at. If it happens to be one of the nodes
  /// that we have dialed into from this topic, send it a "JOIN"
  /// message if our active view is not full yet.
  fn handle_peer_connected(
    &mut self,
    peer: AddressablePeer,
    connection: ConnectionId,
  ) {
    self.handle_non_pending_connects(&peer, connection);
    self.handle_pending_neighbours(&peer, connection);
    self.handle_pending_shuffle_replies(&peer, connection);

    for addr in peer.addresses {
      if let Ok(addr) = addr.try_into() {
        self.pending_dials.remove(&addr);
      }
    }
  }

  fn handle_peer_disconnected(&mut self, peer: PeerId, _: ConnectionId) {
    self.move_active_to_passive(peer);
    self.pending_shuffle_replies.remove(&peer);
    self.pending_disconnects.remove(&peer);
  }

  fn handle_tick(&mut self) {
    // clean up expiring collections
    self.pending_dials.prune_expired();
    self.pending_disconnects.prune_expired();
    self.pending_joins.prune_expired();
    self.pending_neighbours.prune_expired();
    self.pending_shuffle_replies.prune_expired();

    if self.last_shuffle.elapsed() > self.network_config.shuffle_interval {
      self.initiate_shuffle();
    }

    // if we're idle and the active view is not
    // full, then try to connect to one of the
    // passive peers and add them to active view
    if !self.active_view_full()
      && self.pending_dials.is_empty()
      && self.pending_neighbours.is_empty()
    {
      self.try_stabilize_active_view();
    }

    // observability
    gauge!("active_view_size", self.active_peers.len() as f64);
    gauge!("passive_view_size", self.passive_peers.len() as f64);
    gauge!("pending_dials", self.pending_dials.len() as f64);
    gauge!("pending_joins", self.pending_joins.len() as f64);
    gauge!("pending_neighbours", self.pending_neighbours.len() as f64);

    debug!(
      "(a: {}, p: {}, j: {}, n: {}, d: {}, dc: {}, t: {})",
      self.active_peers.len(),
      self.passive_peers.len(),
      self.pending_joins.len(),
      self.pending_neighbours.len(),
      self.pending_dials.len(),
      self.pending_disconnects.len(),
      self.topic_config.name
    );
  }
}

impl TopicInner {
  fn disconnect(
    &mut self,
    peer: PeerId,
    connection: ConnectionId,
    reason: &str,
  ) {
    if !self.pending_disconnects.insert(peer) {
      tracing::debug!(
        "{}: disconnecting {}: {reason}",
        self.topic_config.name,
        peer
      );
      self
        .cmdtx
        .send(Command::Disconnect { connection, peer })
        .expect("topic lifetime < network lifetime");
    }
  }

  fn dial_addr(&mut self, addr: Multiaddr) {
    if let Ok(sockaddr) = addr.clone().try_into() {
      if !self.pending_dials.insert(sockaddr) {
        self
          .cmdtx
          .send(Command::Connect {
            addr,
            topic: self.topic_config.name.clone(),
          })
          .expect("lifetime of network should be longer than topic");
      }
    }
  }

  /// Have we dialed the node and still waiting for the connection
  /// to be established?
  fn is_pending_dial(&self, peer: &AddressablePeer) -> bool {
    for addr in &peer.addresses {
      if let Ok(addr) = addr.try_into() {
        if self.pending_dials.contains(&addr) {
          return true;
        }
      }
    }
    false
  }

  fn ban(&mut self, peer: PeerId, reason: &str) {
    warn!(
      "Peer {peer} banned on topic {}: {reason}",
      self.topic_config.name
    );

    self.active_peers.remove(&peer);
    self.remove_from_passive_view(peer);

    self
      .cmdtx
      .send(Command::BanPeer(peer))
      .expect("topic lifetime < network lifetime");
  }

  pub(super) fn send_message(
    &self,
    peer: PeerId,
    connection: ConnectionId,
    msg: Message,
  ) {
    self
      .cmdtx
      .send(Command::SendMessage {
        peer,
        connection,
        msg,
      })
      .expect("network lifetime > topic lifetime");
  }
}

// add/remove to/from views
impl TopicInner {
  /// Checks if a peer is already in the active view of this topic.
  /// This is used to check if we need to send JOIN message when
  /// the peer is dialed, peers that are active will not get
  /// a JOIN request, otherwise the network will go into endless
  /// join/forward churn.
  fn is_active(&self, peer: &PeerId) -> bool {
    self.active_peers.contains_key(peer)
  }

  /// Starved topics are ones where the active view
  /// doesn't have a minimum set of nodes in it.
  fn active_view_starved(&self) -> bool {
    self.active_peers.len() < self.network_config.min_active_view_size()
  }

  /// Full topics are ones that have their active view full.
  fn active_view_full(&self) -> bool {
    self.active_peers.len() >= self.network_config.optimal_active_view_size()
  }

  fn active_view_overconnected(&self) -> bool {
    self.active_peers.len() > self.network_config.optimal_active_view_size()
  }

  fn add_to_passive_view(&mut self, peer: AddressablePeer) {
    if self
      .passive_peers
      .insert(peer.peer_id, peer.addresses)
      .is_none()
    {
      debug!(
        "{}: added to passive view: {}",
        self.topic_config.name, peer.peer_id
      );

      // if we've reached the passive view limit, remove a random node
      if self.passive_peers.len() > self.network_config.max_passive_view_size()
      {
        let random = *self
          .passive_peers
          .keys()
          .choose(&mut rand::thread_rng())
          .expect("already checked that it is not empty");
        self.remove_from_passive_view(random);
      }
    }
  }

  fn remove_from_passive_view(&mut self, peer: PeerId) {
    self.passive_peers.remove(&peer);
  }

  fn add_to_active_view(
    &mut self,
    peer: AddressablePeer,
    connection: ConnectionId,
  ) {
    if self.is_active(&peer.peer_id) {
      return;
    }

    if peer.peer_id == self.this_node.peer_id {
      return;
    }

    debug!("{}: added to active view: {peer:?}", self.topic_config.name);

    self
      .active_peers
      .insert(peer.peer_id, (connection, peer.addresses.clone()));

    self.remove_from_passive_view(peer.peer_id);
  }

  fn move_active_to_passive(&mut self, peer_id: PeerId) {
    if let Some((_, addresses)) = self.active_peers.remove(&peer_id) {
      debug!(
        "{}: removed from active view: {peer_id:?}",
        self.topic_config.name
      );
      self.add_to_passive_view(AddressablePeer { peer_id, addresses });
    }
  }

  /// Called whenever this node gets a chance to learn about new peers.
  ///
  /// If the active view is not saturated, it will randomly pick a peer
  /// from the passive view and try to add it to the active view.
  fn try_stabilize_active_view(&mut self) {
    if self.active_view_starved()
      && self.pending_dials.is_empty()
      && self.pending_neighbours.is_empty()
    {
      // we're starved, try to connect to a random passive peer
      let random_passive =
        self.passive_peers.iter().choose(&mut thread_rng()).map(
          |(peer, addrs)| AddressablePeer {
            peer_id: *peer,
            addresses: addrs.clone(),
          },
        );

      if let Some(peer) = random_passive {
        self.try_neighbouring_with(peer);
      } else if self.pending_dials.is_empty() && self.pending_joins.is_empty() {
        // this case occurs when we failed to establish
        // a connection with any of the bootstrap nodes
        // and our passive view is empty. retry dialing
        // them and JOIN the topic.
        //
        // We don't know anything about the network yet,
        // and this is the only bit of information that
        // we can use to join it.
        for addr in self.topic_config.bootstrap.clone() {
          self.dial_addr(addr);
        }
      }

      increment_counter!("replenish_active_view");
    } else if self.active_view_overconnected()
      && self.pending_disconnects.is_empty()
    {
      // active view is too large, disconnect a random connection
      let (peerid, (connection, _)) = self
        .active_peers
        .iter()
        .choose(&mut rand::thread_rng())
        .expect("already checked that it is not empty");
      self.disconnect(*peerid, *connection, "overconnected");
    }
  }
}

// HyParView protocol message handlers
impl TopicInner {
  /// Handles JOIN messages.
  ///
  /// When a JOIN request arrives, the receiving node will create a new
  /// FORWARDJOIN and send it to peers in its active view, which
  /// in turn will forward it to all peers in their active view. The forward
  /// operation will repeat to all further active view for N hops. N is set
  /// in the config object ([`forward_join_hops_count`]).
  ///
  /// Each node receiving JOIN or FORWARDJOIN request will send a NEIGHBOR
  /// request to the node attempting to join the topic overlay if its active
  /// view is not saturated. Except nodes on the last hop, if they are saturated
  /// they will move a random node from the active view to their passive view
  /// and establish an active connection with the initiator.
  fn consume_join(
    &mut self,
    sender: PeerId,
    msg: Join,
    connection: ConnectionId,
  ) {
    increment_counter!(
      "received_join",
      "topic" => self.topic_config.name.clone()
    );

    debug!(
      "join request on topic {} from {sender}",
      self.topic_config.name
    );

    if sender != msg.node.peer_id {
      // Seems like an impersonation attempt
      // JOIN messages are not forwarded to
      // other peers as is.
      self.ban(sender, "JOIN impersonation");
      return;
    }

    if self.is_active(&sender) {
      return; // already joined
    }

    // forward join to all active peers
    for (peer, (connection, _)) in self.active_peers.iter() {
      self.send_message(
        *peer,
        *connection,
        Message::new(
          self.topic_config.name.clone(),
          Action::ForwardJoin(ForwardJoin {
            hop: 1,
            node: msg.node.clone(),
          }),
        ),
      );
    }

    // close JOIN connection immediately
    self.disconnect(sender, connection, "initial join");

    if !self.active_view_full() {
      // if our active view is not full, then
      // start adding this peer to our active view
      self.try_neighbouring_with(msg.node.clone());
    }

    // remember this peer
    self.add_to_passive_view(msg.node);
  }

  /// Handles FORWARDJOIN messages.
  ///
  /// Each node receiving FORWARDJOIN checks if its active view is full,
  /// and if there is still space for new nodes, establishes an active
  /// connection with the initiating peer by sending it NEIGHBOR message.
  ///
  /// Then it increments the hop counter on the FORWARDJOIN message and
  /// sends it to all its active peers. This process repeats for N steps
  /// (configured in [`forward_join_hops_count]).
  ///
  /// Nodes on the last hop MUST establish an active view with the initiator,
  /// even if they have to move one of their active connections to passive mode.
  fn consume_forward_join(&mut self, sender: PeerId, msg: ForwardJoin) {
    // only active peers are allowed to send this message
    if !self.is_active(&sender) {
      return;
    }

    increment_counter!(
      "received_forward_join",
      "topic" => self.topic_config.name.clone(),
      "hop" => msg.hop.to_string()
    );

    if sender == msg.node.peer_id {
      // nodes may not send this message for themselves.
      // it has to be innitiated by another peer that received
      // JOIN message. It arrived via a cycle in the peer graph.
      return;
    }

    if msg.node.peer_id == self.this_node.peer_id {
      // cyclic forward join from this node, ignore
      return;
    }

    // we have learned about a new peer, add it to
    // the passive view.
    self.add_to_passive_view(msg.node.clone());

    // if last hop, must create active connection
    if msg.hop as usize >= self.network_config.random_walk_length() {
      if !self.pending_neighbours.contains(&msg.node.peer_id) {
        if self.active_view_full() {
          // our active view is full, need to free up a slot
          let (random_id, rand_conn) = self
            .active_peers
            .iter()
            .choose(&mut rand::thread_rng())
            .map(|(p, (c, _))| (*p, *c))
            .expect("already checked that it is not empty");

          self.disconnect(
            random_id,
            rand_conn,
            "making space for last-hop join request",
          );

          // move the unlucky node to passive view
          self.add_to_passive_view(AddressablePeer {
            peer_id: random_id,
            addresses: self
              .active_peers
              .get(&random_id)
              .map(|(_, addrs)| addrs)
              .expect("chosen by random from existing values")
              .clone(),
          });
        }

        self.try_neighbouring_with(msg.node);
      }
    } else {
      // if not last hop, create an active connection only if our
      // active view is not full and forward the join request to
      // other peers in the active view.
      if !self.active_view_full() {
        self.try_neighbouring_with(msg.node.clone());
      }

      for (peer, (connection, _)) in self.active_peers.iter() {
        if *peer != sender {
          self.send_message(
            *peer,
            *connection,
            Message::new(
              self.topic_config.name.clone(),
              Action::ForwardJoin(ForwardJoin {
                hop: msg.hop + 1,
                node: msg.node.clone(),
              }),
            ),
          );
        }
      }
    }
  }

  /// Handles NEIGHBOR messages.
  ///
  /// This message is send when a node wants to establish an active connection
  /// with the current node. This message is sent as a response to JOIN and
  /// FORWARDJOIN messages initiated by the peer wanting to join the overlay.
  ///
  /// This message is also sent to nodes that are being moved from passive view
  /// to the active view.
  fn consume_neighbor(
    &mut self,
    sender: PeerId,
    msg: Neighbour,
    connection: ConnectionId,
  ) {
    if self.is_active(&sender) {
      return;
    }

    increment_counter!(
      "received_neighbor",
      "topic" => self.topic_config.name.clone()
    );

    if sender != msg.peer.peer_id {
      // impersonation attempt. ban sender
      self.ban(sender, "NEIGHBOUR impersonation");
      return;
    }

    // SECURITY:
    // an attack vector here could be through sybil attack
    // where someone creates many identities and sends
    // many NEIGHBOUR requests with high priority
    // and surrounds a node by taking all its active
    // peer slots.
    if self.active_view_full() && msg.high_priority {
      // our active view is full, need to free up a slot
      let (random_id, (rand_conn, _)) = self
        .active_peers
        .iter()
        .choose(&mut rand::thread_rng())
        .expect("already checked that it is not empty");
      self.disconnect(
        *random_id,
        *rand_conn,
        "high priority neighbour connecting on full active view",
      );
    }

    if !self.active_view_full() {
      self.add_to_active_view(msg.peer, connection);
    } else {
      self.disconnect(
        sender,
        connection,
        "low priority neighbor connecting on full active view",
      );
    }
  }

  /// Handles SHUFFLE messages.
  ///
  /// Every given interval [`Config::shuffle_interval`] a subset of all
  /// nodes ([`Config::shuffle_probability`]) will send a SHUFFLE message
  /// to one randomly chosen peer in its active view and increment the hop
  /// counter.
  ///
  /// Each node that receives a SHUFFLE message will forward it to all its
  /// active peers and increment the hop counter on every hop. When a SHUFFLE
  /// message is received by a peer it adds all unique nodes that are not known
  /// to the peer to its passive view. This is a method of advertising and
  /// discovery of new nodes on the p2p network.
  ///
  /// Each node that receives a SHUFFLE message that replies with SHUFFLEREPLY
  /// with a sample of its own active and passive nodes that were not present
  /// in the SHUFFLE message.
  fn consume_shuffle(&mut self, sender: PeerId, msg: Shuffle) {
    // only active peers are allowed to send this, otherwise
    // its a protocol violation.
    if !self.is_active(&sender) {
      return;
    }

    increment_counter!(
      "received_shuffle",
      "topic" => self.topic_config.name.clone(),
      "peers_count" => msg.peers.len().to_string()
    );

    let recv_peer_ids: HashSet<_> = msg
      .peers
      .iter()
      .choose_multiple(
        // trim to max shuffle size
        &mut thread_rng(),
        self.network_config.shuffle_sample_size(),
      )
      .iter()
      .map(|p| p.peer_id)
      .collect();

    let local_peer_ids: HashSet<_> = self
      .active_peers
      .keys()
      .chain(self.passive_peers.keys())
      .cloned()
      .collect();

    let resp_unique_peers: HashSet<_> = local_peer_ids
      .difference(&recv_peer_ids) // local \ rec
      .cloned()
      .collect();

    gauge!("shuffle_unique_peers", resp_unique_peers.len() as f64);

    let new_peers: HashSet<_> = recv_peer_ids
      .difference(&local_peer_ids) // rec \ local
      .cloned()
      .collect();

    gauge!("shuffle_new_peers", new_peers.len() as f64);

    // all peers are either in our passive view,
    // our local active view, or the list of peers
    // we have just learned about from the shuffle
    // message. Given a peer ID construct a complete
    // peer info from either of those sources.
    macro_rules! collect_peer_addrs {
      ($peer_id:expr) => {
        self
          .passive_peers
          .get(&$peer_id)
          .map(Clone::clone)
          .or_else(|| self.active_peers.get(&$peer_id).map(|(_, v)| v.clone()))
          .or_else(|| {
            Some(
              msg
                .peers
                .get(&AddressablePeer {
                  peer_id: $peer_id,
                  addresses: HashSet::new(),
                })
                .expect("the only remaining possibility")
                .addresses
                .clone(),
            )
          })
          .expect("querying all possible sources")
      };
    }

    // Respond to the shuffle initiator with a list of
    // unique peers that we know about and were not
    // present in their shuffle.
    let shuffle_reply = ShuffleReply {
      peers: resp_unique_peers
        .into_iter()
        .map(|peer_id| AddressablePeer {
          peer_id,
          addresses: collect_peer_addrs!(peer_id),
        })
        .collect(),
    };

    // the new passive view is a random sample over
    // what this node knew before the shuffle and
    // new information learned during this shuffle.
    self.passive_peers = local_peer_ids
      .into_iter()
      .chain(new_peers.into_iter())
      .choose_multiple(
        // random std sample
        &mut thread_rng(),
        self.network_config.max_passive_view_size(),
      )
      .into_iter()
      .map(|peer_id| (peer_id, collect_peer_addrs!(peer_id)))
      .collect();

    // if the initiator is one of our active peers,
    // then just respond to it on the open link.
    if let Some((connection, _)) = self.active_peers.get(&msg.origin.peer_id) {
      self.send_message(
        msg.origin.peer_id,
        *connection,
        Message::new(
          self.topic_config.name.clone(),
          Action::ShuffleReply(shuffle_reply),
        ),
      );

      increment_counter!("shuffle_reply");
    } else {
      // otherwise, open a short-lived connection to
      // the initiator and send the reply.
      self
        .pending_shuffle_replies
        .insert(msg.origin.peer_id, shuffle_reply);

      for addr in &msg.origin.addresses {
        self.dial_addr(addr.clone());
      }
    }

    // forward the shuffle message until
    // hop count reaches shuffle max hops
    if (msg.hop as usize) < self.network_config.random_walk_length() {
      for (peer, (connection, _)) in self.active_peers.iter() {
        if *peer != sender {
          self.send_message(
            *peer,
            *connection,
            Message::new(
              self.topic_config.name.clone(),
              Action::Shuffle(Shuffle {
                hop: msg.hop + 1,
                origin: msg.origin.clone(),
                peers: msg.peers.clone(),
              }),
            ),
          );
        }
      }
    }
  }

  /// Handles SHUFFLEREPLY messages.
  ///
  /// Those messages are sent as responses to SHUFFLE messages to the originator
  /// of the SHUFFLE message. The SHUFFLEREPLY message should contain a sample
  /// of local node's known active and passive peers that were not present in
  /// the received SHUFFLE message.
  fn consume_shuffle_reply(
    &mut self,
    sender: PeerId,
    msg: ShuffleReply,
    connection: ConnectionId,
  ) {
    increment_counter!(
      "received_shuffle_reply",
      "topic" => self.topic_config.name.clone(),
      "peers_count" => msg.peers.len().to_string()
    );

    let new_peers: HashSet<_> = self
      .passive_peers
      .keys()
      .collect::<HashSet<_>>()
      .difference(&msg.peers.iter().map(|p| &p.peer_id).collect::<HashSet<_>>())
      .map(|p| **p)
      .collect();

    gauge!("shuffle_reply_new", new_peers.len() as f64);

    self.passive_peers = self
      .passive_peers
      .keys()
      .chain(new_peers.iter()) // merge what we know with new knowledg
      .choose_multiple( // and sample a random subset of it
        &mut thread_rng(),
        self.network_config.max_passive_view_size(),
      )
      .into_iter()
      .map(|id| {
        (
          *id,
          self
            .passive_peers
            .get(id)
            .map(Clone::clone)
            .or_else(|| {
              Some(
                msg
                  .peers
                  .get(&AddressablePeer {
                    peer_id: *id,
                    addresses: Default::default(),
                  })
                  .expect("covered all sources")
                  .addresses
                  .clone(),
              )
            })
            .expect("all sources covered"),
        )
      })
      .collect();

    if !self.is_active(&sender) {
      self.disconnect(sender, connection, "gossip from inactive shuffle reply");
    }
  }

  /// Invoked when a content is gossiped to this node.
  ///
  /// Those messages are emitted to listeners on this topic events.
  /// The message id is a randomly generated identifier by the originating
  /// node and is used to ignore duplicate messages.
  fn consume_gossip(
    &mut self,
    sender: PeerId,
    msg: Bytes,
    connection: ConnectionId,
  ) {
    // only active peers are allowed to send this message.
    if !self.is_active(&sender) {
      error!("gossip from inactive peer {sender}: {msg:?}");
      self.disconnect(sender, connection, "gossip from inactive peer");
      return;
    }

    gauge!(
      "gossip_size", msg.len() as f64,
      "topic" => self.topic_config.name.clone());
    increment_counter!(
      "gossip_count",
      "topic" => self.topic_config.name.clone());

    for (peer, (connection, _)) in self.active_peers.iter() {
      if *peer != sender {
        self.send_message(
          *peer,
          *connection,
          Message::new(
            self.topic_config.name.clone(),
            Action::Gossip(msg.clone()),
          ),
        );
      }
    }

    self.outmsgs.send(msg.into());
  }
}

impl TopicInner {
  /// Begins the process of adding a peer to the active view. First it
  /// marks the peer as "pending neighbouring" and then dials into the peer,
  /// so when the connection eventually gets established, we will send it a
  /// NEIGHBOUR message and add it to the active view.
  ///
  /// returns true if the neigbouring process was initiated, otherwise returns
  /// false if another similar process is in progress and have not completed
  /// or failed.
  fn try_neighbouring_with(&mut self, peer: AddressablePeer) -> bool {
    // make sure it's not alread an active peer and we're not in the middle of
    // dialing this peer
    if self.is_active(&peer.peer_id) || self.is_pending_dial(&peer) {
      return false;
    }

    // make sure that we haven't already started neighbouring
    // with this peer.
    if !self.pending_neighbours.insert(peer.peer_id) {
      tracing::debug!(
        "{}: initiating NEIGHBOUR with {peer:?}",
        self.topic_config.name
      );
      // dial on all known addresses of this peer
      for addr in peer.addresses {
        self.dial_addr(addr.clone());
      }
      return true;
    }
    false
  }

  fn initiate_shuffle(&mut self) {
    self.last_shuffle = Instant::now();

    // range [0, 1)
    let toss: f32 = StdRng::from_entropy().sample(Standard);
    if (1.0 - toss) > self.network_config.shuffle_probability {
      return; // not this time.
    }

    increment_counter!("shuffles");

    // This is the list of peers that we will exchange
    // with other peers during our shuffle operation.
    let peers_sample = self
      .active_peers
      .keys()
      .chain(self.passive_peers.keys())
      .choose_multiple(
        &mut thread_rng(),
        self.network_config.shuffle_sample_size(),
      );

    gauge!("shuffle_size", peers_sample.len() as f64);

    // chose a random peer from the active view to initiate the shuffle with
    if let Some((peer, (connection, _))) =
      self.active_peers.iter().choose(&mut thread_rng())
    {
      self.send_message(
        *peer,
        *connection,
        Message::new(
          self.topic_config.name.clone(),
          Action::Shuffle(Shuffle {
            hop: 1,
            origin: self.this_node.clone(),
            peers: peers_sample
              .into_iter()
              .map(|peer_id| AddressablePeer {
                peer_id: *peer_id,
                addresses: self
                  .active_peers
                  .get(peer_id)
                  .map(|(_, addrs)| addrs)
                  .or_else(|| self.passive_peers.get(peer_id))
                  .expect("")
                  .clone(),
              })
              .collect(),
          }),
        ),
      );
    }
  }
}

impl Stream for Topic {
  type Item = Vec<u8>;

  fn poll_next(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<Self::Item>> {
    self.inner.write().outmsgs.poll_recv(cx)
  }
}
