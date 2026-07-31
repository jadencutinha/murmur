//! Encoding of KV operations into (and out of) Raft log entries.
//!
//! Raft treats a log entry's `command` as opaque bytes; murmur fills those bytes
//! with a serialized [`proto::Command`]. The leader encodes a command when a
//! client request arrives; every node decodes and applies it once the entry
//! commits. Using the generated protobuf message as the wire form keeps the
//! encoding versionable and self-describing, and means Raft never has to know
//! what a command *is*.

use prost::Message;

use crate::proto::{Command, Op};

/// Serialize a command to the bytes stored in a log entry.
pub fn encode(command: &Command) -> Vec<u8> {
    command.encode_to_vec()
}

/// Deserialize a command from a log entry's bytes.
pub fn decode(bytes: &[u8]) -> Result<Command, prost::DecodeError> {
    Command::decode(bytes)
}

/// A read of `key`, routed through the log so it observes a linearizable point
/// in the committed order. Reads are idempotent, so they carry no client
/// identity (`client_id`/`seq` stay zero) and are never deduplicated.
pub fn get(key: Vec<u8>) -> Command {
    Command { op: Op::Get as i32, key, value: Vec::new(), client_id: 0, seq: 0, nonce: 0 }
}

/// A write of `key` = `value`, tagged with the caller's identity for dedup.
pub fn put(key: Vec<u8>, value: Vec<u8>, client_id: u64, seq: u64) -> Command {
    Command { op: Op::Put as i32, key, value, client_id, seq, nonce: 0 }
}

/// An append of `value` to `key`'s current contents (non-idempotent).
pub fn append(key: Vec<u8>, value: Vec<u8>, client_id: u64, seq: u64) -> Command {
    Command { op: Op::Append as i32, key, value, client_id, seq, nonce: 0 }
}

/// A removal of `key`, tagged with the caller's identity for dedup.
pub fn delete(key: Vec<u8>, client_id: u64, seq: u64) -> Command {
    Command { op: Op::Delete as i32, key, value: Vec::new(), client_id, seq, nonce: 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_round_trip_through_bytes() {
        for cmd in [
            get(b"k".to_vec()),
            put(b"k".to_vec(), b"v".to_vec(), 7, 3),
            append(b"k".to_vec(), b"v".to_vec(), 7, 4),
            delete(b"k".to_vec(), 7, 5),
        ] {
            let bytes = encode(&cmd);
            assert_eq!(decode(&bytes).unwrap(), cmd);
        }
    }

    #[test]
    fn op_discriminants_are_recoverable() {
        let cmd = append(b"x".to_vec(), b"1".to_vec(), 1, 1);
        assert_eq!(Op::try_from(cmd.op), Ok(Op::Append));
    }
}
