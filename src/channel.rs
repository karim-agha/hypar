use {
  std::{
    fmt::Debug,
    task::{Context, Poll},
  },
  tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

/// FIFO queue used in async streams.
///
/// This type symantically behaves like a fifo queue and it enables
/// signalling futures whenever new elements are available through a Waker.
pub struct Channel<T: Send + Sync + Debug> {
  tx: UnboundedSender<T>,
  rx: UnboundedReceiver<T>,
}

impl<T: Send + Sync + Debug> Channel<T> {
  pub fn new() -> Self {
    let (tx, rx) = unbounded_channel();
    Self { tx, rx }
  }

  pub fn send(&self, message: T) {
    self
      .tx
      .send(message)
      .expect("lifetime of receiver is equal to sender")
  }

  pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
    self.rx.poll_recv(cx)
  }

  pub fn sender(&self) -> UnboundedSender<T> {
    self.tx.clone()
  }

  pub fn split(self) -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    (self.tx, self.rx)
  }
}
