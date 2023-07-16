use {
  crate::{upgrade, wire},
  asynchronous_codec::{Bytes, BytesMut, Decoder, Encoder},
  unsigned_varint::codec,
};

/// All messages are length-prefixed MessagePack serialized bytes.
pub struct Codec {
  /// prefix all network frames with varint of their length.
  length_prefix: codec::UviBytes,
}

impl Codec {
  /// Creates a wrapper codec that prefixes the serialized message
  /// bytes with a varint of its length. This is also used to
  /// invalidate any messages that exceed the allowed message size.
  ///
  /// Messages are serialized using MessagePack.
  ///
  /// MessagePack on its own doesn't know the size of the message
  /// without actually serializing/deserializing an object, thus
  /// the varint prefix.
  pub fn new(max_message_len: usize) -> Self {
    Self {
      length_prefix: {
        let mut length_codec = codec::UviBytes::default();
        length_codec.set_max_len(max_message_len);
        length_codec
      },
    }
  }
}

impl Encoder for Codec {
  type Error = upgrade::Error;
  type Item = wire::Message;

  fn encode(
    &mut self,
    item: Self::Item,
    dst: &mut BytesMut,
  ) -> Result<(), Self::Error> {
    // prepend buffer len varint
    self
      .length_prefix
      .encode(Bytes::from(rmp_serde::to_vec(&item)?), dst)
      .map_err(|_| upgrade::Error::MaxTransmissionSizeExceeded)
  }
}

impl Decoder for Codec {
  type Error = upgrade::Error;
  type Item = wire::Message;

  fn decode(
    &mut self,
    src: &mut BytesMut,
  ) -> Result<Option<Self::Item>, Self::Error> {
    let packet =
      match self.length_prefix.decode(src).map_err(|e| match e.kind() {
        std::io::ErrorKind::PermissionDenied => {
          upgrade::Error::MaxTransmissionSizeExceeded
        }
        _ => upgrade::Error::Io(e),
      })? {
        Some(p) => p,
        None => return Ok(None),
      };

    Ok(Some(rmp_serde::from_slice(&packet)?))
  }
}
