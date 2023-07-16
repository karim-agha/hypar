use {
  crate::{cache::ExpiringMap, wire::AddressablePeer, Config},
  libp2p::{core::connection::ConnectionId, Multiaddr, PeerId},
  std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    net::IpAddr,
  },
};

type TopicName = String;

/// Used to track peers connections and their assosiation with
/// joined topics on this node. It is used to properly signal
/// PeerConnected and PeerDisconnected to topics.
///
/// A pair of peers might have multiple concurrent substreams.
pub struct Muxer {
  /// a mapping of peer id to all joined subtopics.
  /// The value on this map is a tuple of the substream id
  /// and the topic name.
  assigned: HashMap<PeerId, HashMap<ConnectionId, TopicName>>,

  /// Peers that have successfully established a connection with the
  /// current node but have not yet communicated on any topic. We still
  /// don't know the mapping between ConnectionId and TopicName. That
  /// mapping will be discovered after a peer sends their first message.
  unassigned: HashMap<PeerId, (AddressablePeer, HashSet<ConnectionId>)>,

  /// Keeps track of topics that have requested connections
  /// to a given peer but a connection was not established yet.
  requested_dials: HashMap<IpAddress, HashMap<TopicName, Multiaddr>>,

  /// Dials currently in progress groupped by the destination IP
  /// address (no port).
  ongoing_dials: ExpiringMap<IpAddress, TopicName>,
}

impl Muxer {
  pub fn new(config: &Config) -> Self {
    Self {
      assigned: HashMap::new(),
      unassigned: HashMap::new(),
      requested_dials: HashMap::new(),
      ongoing_dials: ExpiringMap::new(config.pending_timeout),
    }
  }

  /// Registers a new connection from a peer.
  ///
  /// This is called when a remote peer establishes a connection with the
  /// local node but we still don't know which topic it is communicating on.
  pub fn register(&mut self, from: AddressablePeer, id: ConnectionId) {
    if let Some((_, conns)) = self.unassigned.get_mut(&from.peer_id) {
      conns.insert(id);
    } else {
      self
        .unassigned
        .insert(from.peer_id, (from.clone(), [id].into_iter().collect()));
    }
  }

  pub fn disconnect(&mut self, peer: PeerId, connection: ConnectionId) {
    if let Some(peer_conns) = self.assigned.get_mut(&peer) {
      peer_conns.remove(&connection);
      if peer_conns.is_empty() {
        self.assigned.remove(&peer);
      }
    } else if let Some((_, peer_conns)) = self.unassigned.get_mut(&peer) {
      peer_conns.remove(&connection);
      if peer_conns.is_empty() {
        self.unassigned.remove(&peer);
      }
    }
  }

  /// Called when a remote node is dialed by a topic.
  /// At this stage we still don't know what connection id will
  /// be assigned to this link and what is the peer id.
  ///
  /// Once the first message is sent or received on the established
  /// connection, then we will discover the mapping of
  /// connection_id <--> topic for this peer.
  pub fn put_dial(&mut self, addr: Multiaddr, topic: TopicName) {
    let exactaddr = addr.clone();
    if let Ok(socketaddr) = addr.try_into() {
      if let Some(topics) = self.requested_dials.get_mut(&socketaddr) {
        topics.insert(topic, exactaddr);
      } else {
        self
          .requested_dials
          .insert(socketaddr, [(topic, exactaddr)].into_iter().collect());
      }
    }
  }

  pub fn poll_dial(&self) -> Option<(TopicName, Multiaddr)> {
    if let Some((_, dial)) = self.requested_dials.iter().next() {
      dial.iter().next().map(|(k, v)| (k.clone(), v.clone()))
    } else {
      None
    }
  }

  pub fn next_dial(&mut self, addr: &Multiaddr, topic: &TopicName) -> bool {
    if let Ok(socketaddr) = addr.try_into() {
      if !self.ongoing_dials.contains_key(&socketaddr) {
        if let Some(requested) = self.requested_dials.get_mut(&socketaddr) {
          if requested.remove(topic).is_some() {
            if requested.is_empty() {
              self.requested_dials.remove(&socketaddr);
            }
            self.ongoing_dials.insert(socketaddr, topic.clone());
            return true;
          }
        }
      }
    }
    false
  }

  /// If some topic dialed one of this peer addresses, then return
  /// the topic name and remove the dial entry from pending dials.
  pub fn match_dial(
    &mut self,
    peer: &AddressablePeer,
    connection: ConnectionId,
  ) -> Option<TopicName> {
    for addr in &peer.addresses {
      if let Ok(socketaddr) = addr.try_into() {
        if let Some(topic) = self.ongoing_dials.remove(&socketaddr) {
          // automatically assign this connection id to
          // the topic that dialed it.
          self
            .assigned
            .entry(peer.peer_id)
            .and_modify(|conns| {
              conns.insert(connection, topic.clone());
            })
            .or_insert_with(|| {
              [(connection, topic.clone())].into_iter().collect()
            });

          return Some(topic);
        }
      }
    }

    None
  }

  /// Invoked when we discover the topic of a given connection to a peer.
  /// This happens after sending or receiving the first message on the
  /// connection.
  pub fn assign(
    &mut self,
    peer: PeerId,
    connection_id: ConnectionId,
    topic: &TopicName,
  ) -> Option<AddressablePeer> {
    if let Some((addrpeer, conns)) = self.unassigned.get_mut(&peer) {
      if conns.remove(&connection_id) {
        match self.assigned.entry(peer) {
          Entry::Occupied(mut o) => {
            o.get_mut().insert(connection_id, topic.clone());
          }
          Entry::Vacant(v) => {
            v.insert([(connection_id, topic.clone())].into_iter().collect());
          }
        };

        let addrpeer = addrpeer.clone();

        if conns.is_empty() {
          self.unassigned.remove(&peer);
        }

        return Some(addrpeer);
      }
    }

    None
  }

  /// Given a peer id and a connection id,
  /// returns the topic name of the connection.
  pub fn resolve_topic(
    &self,
    peer: &PeerId,
    connection: &ConnectionId,
  ) -> Option<&TopicName> {
    self
      .assigned
      .get(peer)
      .and_then(|conns| conns.get(connection))
  }

  pub fn assigned_count(&self) -> usize {
    self.assigned.len()
  }

  pub fn unassigned_count(&self) -> usize {
    self.unassigned.len()
  }

  pub fn prune_expired(&mut self) {
    self.ongoing_dials.prune_expired();
  }
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq)]
pub struct IpAddress(IpAddr);

impl TryFrom<Multiaddr> for IpAddress {
  type Error = ();

  fn try_from(value: Multiaddr) -> Result<Self, Self::Error> {
    (&value).try_into()
  }
}

impl TryFrom<&Multiaddr> for IpAddress {
  type Error = ();

  fn try_from(addr: &Multiaddr) -> Result<Self, Self::Error> {
    if let Some(comp) = addr.iter().next() {
      return match comp {
        libp2p::multiaddr::Protocol::Ip4(addr) => {
          Ok(IpAddress(IpAddr::V4(addr)))
        }
        libp2p::multiaddr::Protocol::Ip6(addr) => {
          Ok(IpAddress(IpAddr::V6(addr)))
        }
        _ => Err(()),
      };
    }

    Err(())
  }
}
