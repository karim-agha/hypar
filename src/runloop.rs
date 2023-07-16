use {
  crate::{
    behaviour::Behaviour,
    channel::Channel,
    network::{self, Error},
    wire::Message,
    Config,
  },
  futures::StreamExt,
  libp2p::{
    core::{
      connection::ConnectionId,
      transport::timeout::TransportTimeout,
      upgrade::Version,
    },
    dns::TokioDnsConfig,
    identity::Keypair,
    noise::{self, NoiseConfig, X25519Spec},
    swarm::{SwarmBuilder, SwarmEvent},
    tcp,
    yamux::YamuxConfig,
    Multiaddr,
    PeerId,
    Swarm,
    Transport,
  },
  metrics::increment_counter,
  tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
  },
  tracing::error,
};

/// Low-level network commands.
///
/// At this level of abstraction there is no notion of topics
/// or any other high-level concepts. Here we are dealing with
/// raw connections to peers, sending and receiving streams of
/// bytes.
#[derive(Debug, Clone)]
pub enum Command {
  /// Establishes a long-lived TCP connection with a peer.
  ///
  /// If a connection already exists with the given address,
  /// then its refcount is incremented by 1.
  ///
  /// This happens when a peer is added to the active view
  /// of one of the topics.
  Connect(Multiaddr),

  /// Disconnects from a peer.
  ///
  /// First it will decrement the recount on a connection with
  /// the peer, and if it reaches zero then the connection gets closed.
  Disconnect(PeerId, ConnectionId),

  /// Bans a peer from connecting to this node.
  ///
  /// This happens when a violation of the network protocol
  /// is detected. Banning a peer will also automatically forcefully
  /// disconnect it from all topics.
  ///
  /// Trying to connect to a peer on an unexpected topic is also
  /// considered a violation of the protocol and gets the sender
  /// banned.
  BanPeer(PeerId),

  /// Sends a message to one peer in the active view of
  /// one of the topics.
  SendMessage {
    peer: PeerId,
    connection: ConnectionId,
    msg: Message,
  },
}

/// Manages the event loop that drives the network layer.
pub(crate) struct Runloop {
  cmdtx: UnboundedSender<Command>,
}

impl Runloop {
  pub fn new(
    config: &Config,
    keypair: Keypair,
    netcmdtx: UnboundedSender<network::Command>,
  ) -> Result<Self, Error> {
    let (tx, rx) = Channel::new().split();
    start_network_runloop(config, keypair, rx, netcmdtx)?;
    Ok(Self { cmdtx: tx })
  }

  pub fn send_command(&self, command: Command) {
    self.cmdtx.send(command).expect("runloop thread died");
  }
}

fn build_swarm(
  config: &Config,
  keypair: Keypair,
) -> Result<Swarm<Behaviour>, Error> {
  // TCP transport with DNS resolution, NOISE encryption and Yammux
  // substream multiplexing.
  let transport = {
    let transport =
      TokioDnsConfig::system(libp2p::tcp::tokio::Transport::new(
        tcp::Config::new().port_reuse(false).nodelay(true),
      ))?;

    let noise_keys =
      noise::Keypair::<X25519Spec>::new().into_authentic(&keypair)?;

    // use network-wide timeout
    TransportTimeout::new(transport, config.pending_timeout)
      .upgrade(Version::V1)
      .authenticate(NoiseConfig::xx(noise_keys).into_authenticated())
      .multiplex(YamuxConfig::default())
      .boxed()
  };

  Ok(
    SwarmBuilder::with_tokio_executor(
      transport, //
      Behaviour::new(config.clone()),
      keypair.public().into(),
    )
    .build(),
  )
}

fn start_network_runloop(
  config: &Config,
  keypair: Keypair,
  cmdrx: UnboundedReceiver<Command>,
  netcmdtx: UnboundedSender<network::Command>,
) -> Result<JoinHandle<()>, Error> {
  // Libp2p network state driver and event loop
  let mut swarm = build_swarm(config, keypair)?;

  // instruct the libp2p engine to accept connections
  // on all configured addresses and ports.
  //
  // The actual sockets will open once we start polling
  // the swarm on a separate thread.
  for addr in &config.listen_addrs {
    swarm.listen_on(addr.clone())?;
  }

  let mut cmdrx = cmdrx;
  Ok(tokio::spawn(async move {
    loop {
      tokio::select! {
        Some(event) = swarm.next() => {
          if let SwarmEvent::Behaviour(event) = event {
            // forward all events to the [`Network`] object and
            // handle it there. This loop is not responsible for
            // any high-level logic except routing commands and
            // events between network foreground and background
            // threads.
            if let Err(e) = netcmdtx.send(network::Command::InjectEvent(event)) {
              error!("Terminating network thread: {e:?}. No further data will be \
                      sent or received over p2p.");
              break;
            }
          }
        }

        Some(command) = cmdrx.recv() => {
          match command {
            Command::Connect(addr) => {
              if let Err(err) = swarm.dial(addr) {
                increment_counter!("dial_errors");
                error!("Failed to dial peer: {err:?}");
              }
            }
            Command::Disconnect(peer, connection) => {
              swarm.behaviour().disconnect_from(peer, connection);
            }
            Command::SendMessage { peer, connection, msg } => {
              increment_counter!(
                "messages_sent",
                "peer" => peer.to_string(),
                "topic" => msg.topic.clone()
              );
              swarm.behaviour().send_to(peer, connection, msg);
            }
            Command::BanPeer(peer) => {
              increment_counter!(
                "banned_peers",
                "peer" => peer.to_base58()
              );
              swarm.ban_peer_id(peer);
            }
          }
        }
      };
    }
  }))
}
