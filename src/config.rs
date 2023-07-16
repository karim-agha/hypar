use {libp2p::Multiaddr, std::time::Duration};

/// Network wide configuration across all topics.
#[derive(Debug, Clone)]
pub struct Config {
  /// Estimated number of online nodes joining one topic
  pub network_size: usize,

  /// HyParView Active View constant
  /// active view size = Ln(N) + C
  ///
  /// This is the fan-out of the node.
  pub active_view_constant: usize,

  /// If the active view size is smaller than
  /// this percentage of the maximum view size,
  /// then all neighbour requests will be high
  /// priority.
  pub active_view_starve_factor: f64,

  /// HyParView Passive View constant
  /// active view size = C * Ln(N)
  pub passive_view_factor: usize,

  /// Maximum size of a message, this applies to
  /// control and payload messages
  pub max_transmit_size: usize,

  /// How often a peer shuffle happens
  /// with a random active peer
  pub shuffle_interval: Duration,

  /// If it has come time to perform shuffle, this
  /// specifies the probability that a shuffle will
  /// actually ocurr. Valid values are 0.0 - 1.0.
  ///
  /// This parameter is used in cases when a network
  /// peers don't all shuffle at the same time if they
  /// have the same [`shuffle_interval`] specified.
  ///
  /// Shuffle from other peers will populate the passive
  /// view anyway.
  pub shuffle_probability: f32,

  /// Local network addresses this node will listen on for incoming
  /// connections. By default it will listen on all available IPv4 and IPv6
  /// addresses on port 44668.
  pub listen_addrs: Vec<Multiaddr>,

  /// How long identical messages will be ignored
  /// by peers if the arrive at the same node.
  ///
  /// In a p2p network the same message could very likely
  /// arrive at some node multiple times if there is a cycle
  /// in the P2P connectivity graph. If this value is set, then
  /// identical messages will be ignored for some time.
  ///
  /// Note: Some higher level protocols might need to see duplicate
  /// values to optimize the broadcast overlay.
  pub dedupe_interval: Option<Duration>,

  /// How long it takes asynchronous network IO operations to timeout
  /// and considered failed.
  ///
  /// This includes waiting for Dial operations to complete, waiting
  /// for shuffle response connections, etc.
  pub pending_timeout: Duration,

  /// This is a periodic event that triggers all topics to perform
  /// maintenance tasks. It gets emitted to topics regardless of
  /// other topic activity.
  pub maintenance_tick_interval: Duration,
}

impl Config {
  pub fn optimal_active_view_size(&self) -> usize {
    ((self.network_size as f64).log2() + self.active_view_constant as f64)
      .round() as usize
  }

  /// A node is considered starving when it's active view size is less than
  /// this value. It will try to maintain half at least
  /// active_view_starve_factor to achieve minimum level of connection
  /// redundancy.
  ///
  /// Two thresholds allow to avoid cyclical connections and disconnections when
  /// new nodes are connected to a group of overconnected nodes.
  pub fn min_active_view_size(&self) -> usize {
    (self.optimal_active_view_size() as f64 * self.active_view_starve_factor)
      .ceil() as usize
  }

  pub fn max_passive_view_size(&self) -> usize {
    self.optimal_active_view_size() * self.passive_view_factor
  }

  pub fn random_walk_length(&self) -> usize {
    (self.network_size as f64).log10().ceil() as usize
  }

  /// The maximum number of peers to send from all views
  /// in each shuffle operation.
  ///
  /// This value is set to half the passive view size,
  /// to make sure that we still retain a portion of
  /// the old passive peers after receiving info about
  /// new ones.
  ///
  /// This applies to incoming and outgoing shuffles,
  /// for outgoing, this is the numbe of randomly chosen
  /// peers that will be sent, and for incoming this is
  /// the number of randomly chosen peers that will be
  /// considered from any incoming shuffle.
  pub fn shuffle_sample_size(&self) -> usize {
    self.max_passive_view_size().div_euclid(2)
  }
}

impl Default for Config {
  fn default() -> Self {
    Self {
      network_size: 100,
      active_view_constant: 1,
      active_view_starve_factor: 0.75, // 75%
      passive_view_factor: 6,
      shuffle_probability: 0.3, // shuffle 30% of the time
      shuffle_interval: Duration::from_secs(180), // 3 minutes
      maintenance_tick_interval: Duration::from_secs(5),
      pending_timeout: Duration::from_secs(15),
      dedupe_interval: Some(Duration::from_secs(5)),
      max_transmit_size: 1024 * 1024, // 1MB
      listen_addrs: vec![
        "/ip4/0.0.0.0/tcp/44668".parse().unwrap(),
        "/ip6/::/tcp/44668".parse().unwrap(),
      ],
    }
  }
}
