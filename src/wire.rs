//! Defines the wire binary protocol messages structure for p2p communication
//! This protocol implements the following work:
//! https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf
//! by Joao Leitao el at.

use {
  bytes::Bytes,
  libp2p::{Multiaddr, PeerId},
  multihash::{Hasher, Multihash, MultihashDigest, Sha3_256},
  once_cell::sync::OnceCell,
  serde::{Deserialize, Serialize},
  std::collections::HashSet,
};

/// Represents a member of the p2p network
/// with a list of all known physical addresses that
/// can be used to reach it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressablePeer {
  /// libp2p encoded version of node [`Address`]
  pub peer_id: PeerId,

  /// All known physical address that can be used
  /// to reach this peer. Not all of them will be
  /// accessible from all locations, so the protocol
  /// will try to connecto to any of the addresses listed here.
  pub addresses: HashSet<Multiaddr>,
}

impl Eq for AddressablePeer {}
impl PartialEq for AddressablePeer {
  fn eq(&self, other: &Self) -> bool {
    self.peer_id == other.peer_id
  }
}

impl From<PeerId> for AddressablePeer {
  fn from(value: PeerId) -> Self {
    AddressablePeer {
      peer_id: value,
      addresses: [].into_iter().collect(),
    }
  }
}

impl std::hash::Hash for AddressablePeer {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.peer_id.hash(state);
  }
}

/// Message sent to a bootstrap node to initiate network join
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
  /// Identity and address of the local node that is trying
  /// to join the p2p network.
  pub node: AddressablePeer,
}

/// Message forwarded to active peers of the bootstrap node.
///
/// Nodes that receive this message will attempt to establish
/// an active connection with the node initiating the JOIN
/// procedure. They will send a [`Neighbor`] message to
/// the joining node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardJoin {
  /// Hop counter. Incremented with every network hop.
  pub hop: u16,

  /// Identity and address of the local node that is trying
  /// to join the p2p network.
  pub node: AddressablePeer,
}

/// Sent as a response to JOIN, FORWARDJOIN to the initating node,
/// or if a node is being moved from passive to active view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Neighbour {
  /// Identity and address of the peer that is attempting
  /// to add this local node to its active view.
  pub peer: AddressablePeer,

  /// High-priority NEIGHBOR requests are sent iff the sender
  /// has zero peers in their active view.
  pub high_priority: bool,
}

/// This message is sent periodically by a subset of
/// peers to propagate info about peers known to them
/// to other peers in the network.
///
/// This message is forwarded for up to N hops.
/// N is configurable in [`network::Config`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shuffle {
  /// Hop counter. Incremented with every network hop.
  pub hop: u16,

  /// Identity and addresses of the node initiating the shuffle.
  pub origin: AddressablePeer,

  /// A sample of known peers to the shuffle originator.
  pub peers: HashSet<AddressablePeer>,
}

/// Sent as a response to SHUFFLE to the shuffle originator.
///
/// Exchanges deduplicated entries about peers known to this
/// local node. This reply is sent by every node that receives
/// the shuffle message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleReply {
  /// A sample of known peers to the local node minus all
  /// nodes listed in the SHUFFLE message.
  pub peers: HashSet<AddressablePeer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
  Join(Join),
  ForwardJoin(ForwardJoin),
  Neighbour(Neighbour),
  Shuffle(Shuffle),
  ShuffleReply(ShuffleReply),
  Gossip(Bytes),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
  pub topic: String,
  pub action: Action,

  #[serde(skip)]
  hash_cache: OnceCell<Multihash>,
}

impl Message {
  pub fn hash(&self) -> &Multihash {
    self.hash_cache.get_or_init(|| {
      let mut hasher = Sha3_256::default();
      hasher.update(
        &rmp_serde::to_vec(self) //
          .expect("all fields have serializable members"),
      );
      multihash::Code::Sha3_256
        .wrap(hasher.finalize())
        .expect("hash length matches hashcode")
    })
  }

  pub fn new(topic: String, action: Action) -> Self {
    Self {
      topic,
      action,
      hash_cache: OnceCell::default(),
    }
  }
}

impl std::fmt::Debug for Message {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Message")
      .field("topic", &self.topic)
      .field("action", &self.action)
      .finish()
  }
}
