use {
  crate::codec::Codec,
  asynchronous_codec::Framed,
  futures::{future, AsyncRead, AsyncWrite},
  libp2p::{core::UpgradeInfo, InboundUpgrade, OutboundUpgrade},
  std::{future::Future, iter, pin::Pin},
  thiserror::Error,
};

#[derive(Debug, Error)]
pub enum Error {
  #[error("Exceeded maximum transmission size")]
  MaxTransmissionSizeExceeded,

  #[error("IO Error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Serialization error: {0}")]
  Serialization(#[from] rmp_serde::encode::Error),

  #[error("Deserialization error: {0}")]
  Deserialization(#[from] rmp_serde::decode::Error),
}

#[derive(Debug, Clone)]
pub struct ProtocolUpgrade {
  max_transmit_size: usize,
}

impl ProtocolUpgrade {
  pub fn new(max_transmit_size: usize) -> Self {
    Self { max_transmit_size }
  }
}

impl UpgradeInfo for ProtocolUpgrade {
  type Info = &'static [u8];
  type InfoIter = iter::Once<Self::Info>;

  fn protocol_info(&self) -> Self::InfoIter {
    // unique libp2p protocol identifier used when connecting
    // to other nodes and negotiating common protocols between
    // both endpoints.
    iter::once(b"/anoma/gossip/1.0")
  }
}

type NetworkFrame<Socket> = Framed<Socket, Codec>;
type UpgradeResult<Socket> = Result<NetworkFrame<Socket>, Error>;

/// Invoked when a remote node is trying to connect to us
/// This logic is used to start the protocol-specific handshake
impl<Socket> InboundUpgrade<Socket> for ProtocolUpgrade
where
  Socket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
  type Error = Error;
  type Future = Pin<Box<dyn Future<Output = UpgradeResult<Socket>> + Send>>;
  type Output = NetworkFrame<Socket>;

  // handshake succeeded, both endpoints support this protocol
  fn upgrade_inbound(self, socket: Socket, _: Self::Info) -> Self::Future {
    Box::pin(future::ok(Framed::new(
      socket,
      Codec::new(self.max_transmit_size),
    )))
  }
}

/// Invoked when we are trying to connect to a remote node.
/// This logic is used to start the protocol-specific handshake
impl<Socket> OutboundUpgrade<Socket> for ProtocolUpgrade
where
  Socket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
  type Error = Error;
  type Future = Pin<Box<dyn Future<Output = UpgradeResult<Socket>> + Send>>;
  type Output = NetworkFrame<Socket>;

  // handshake succeeded, both endpoints support this protocol
  fn upgrade_outbound(self, socket: Socket, _: Self::Info) -> Self::Future {
    Box::pin(future::ok(Framed::new(
      socket,
      Codec::new(self.max_transmit_size),
    )))
  }
}
