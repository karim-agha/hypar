use std::{
  collections::HashMap,
  hash::Hash,
  time::{Duration, Instant},
};

#[derive(Debug, Clone)]
struct Timestamped<T>(T, Instant);

impl<T> Eq for Timestamped<T> {}
impl<T> PartialEq for Timestamped<T> {
  fn eq(&self, other: &Self) -> bool {
    self.1 == other.1
  }
}

impl<T> PartialOrd for Timestamped<T> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    self.1.partial_cmp(&other.1)
  }
}

#[derive(Debug)]
pub struct ExpiringMap<K, V>
where
  K: Eq + Hash,
{
  lifespan: Duration,
  data: HashMap<K, Timestamped<V>>,
}

impl<K: Eq + Hash, V> ExpiringMap<K, V> {
  pub fn new(lifespan: Duration) -> Self {
    Self {
      lifespan,
      data: HashMap::new(),
    }
  }

  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn insert(&mut self, key: K, value: V) -> Option<V> {
    let value = Timestamped(value, Instant::now());
    if let Some(old) = self.data.insert(key, value) {
      if old.1.elapsed() > self.lifespan {
        return None;
      }
      return Some(old.0);
    }
    None
  }

  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }

  pub fn get(&self, key: &K) -> Option<&V> {
    if let Some(value) = self.data.get(key) {
      if value.1.elapsed() > self.lifespan {
        return None;
      }
      return Some(&value.0);
    }
    None
  }

  pub fn contains_key(&self, key: &K) -> bool {
    self.get(key).is_some()
  }

  pub fn remove(&mut self, key: &K) -> Option<V> {
    if let Some(value) = self.data.remove(key) {
      if value.1.elapsed() > self.lifespan {
        return None;
      }
      return Some(value.0);
    }
    None
  }

  pub fn prune_expired(&mut self) {
    self.data.retain(|_, v| v.1.elapsed() < self.lifespan);
  }
}

#[derive(Debug)]
pub struct ExpiringSet<T>
where
  T: Eq + Hash,
{
  inner: ExpiringMap<T, ()>,
}

impl<T: Eq + Hash> ExpiringSet<T> {
  pub fn new(lifespan: Duration) -> Self {
    Self {
      inner: ExpiringMap::new(lifespan),
    }
  }

  pub fn len(&self) -> usize {
    self.inner.len()
  }

  pub fn is_empty(&self) -> bool {
    self.inner.is_empty()
  }

  pub fn insert(&mut self, key: T) -> bool {
    self.inner.insert(key, ()).is_some()
  }

  pub fn contains(&self, key: &T) -> bool {
    self.inner.get(key).is_some()
  }

  pub fn remove(&mut self, key: &T) -> bool {
    self.inner.remove(key).is_some()
  }

  pub fn prune_expired(&mut self) {
    self.inner.prune_expired();
  }
}
