//! The row messages of the `pgoutput` plugin (protocol version 1), read
//! straight from the bytes the replication connection delivers. Cells are
//! borrowed from the message, never copied here.

use std::fmt;

/// Why a message could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageError(pub String);

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unreadable pgoutput message: {}", self.0)
    }
}

impl std::error::Error for MessageError {}

/// One column of a relation as its `Relation` message describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
}

/// A relation's description, sent before its first row in a session and
/// again whenever its shape changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<RelationColumn>,
}

/// One cell of a tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cell<'a> {
    Null,
    /// A large value the change did not touch, left out of the message.
    Unchanged,
    /// The value in Postgres's text form, for a type without a binary
    /// send function.
    Text(&'a [u8]),
    /// The value in Postgres's binary form.
    Binary(&'a [u8]),
}

/// A row message or a relation description; every other message is
/// [`Message::Other`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<'a> {
    Relation(Relation),
    Insert {
        relation: u32,
        new: Vec<Cell<'a>>,
    },
    Update {
        relation: u32,
        old: Option<Vec<Cell<'a>>>,
        new: Vec<Cell<'a>>,
    },
    Delete {
        relation: u32,
        old: Vec<Cell<'a>>,
    },
    Truncate,
    Other,
}

/// A cursor over a message's bytes.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], MessageError> {
        let end = self
            .at
            .checked_add(count)
            .ok_or_else(|| MessageError("length overflow".to_owned()))?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| MessageError("truncated".to_owned()))?;
        self.at = end;
        Ok(slice)
    }

    fn byte(&mut self) -> Result<u8, MessageError> {
        self.take(1).map(|slice| slice[0])
    }

    fn u16(&mut self) -> Result<u16, MessageError> {
        let slice = self.take(2)?;
        Ok(u16::from_be_bytes([slice[0], slice[1]]))
    }

    fn u32(&mut self) -> Result<u32, MessageError> {
        let slice = self.take(4)?;
        Ok(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
    }

    fn cstring(&mut self) -> Result<String, MessageError> {
        let rest = self.bytes.get(self.at..).unwrap_or_default();
        let length = rest
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| MessageError("unterminated string".to_owned()))?;
        let text = std::str::from_utf8(self.take(length)?)
            .map_err(|_| MessageError("a name is not UTF-8".to_owned()))?
            .to_owned();
        self.take(1)?;
        Ok(text)
    }

    fn tuple(&mut self) -> Result<Vec<Cell<'a>>, MessageError> {
        let count = usize::from(self.u16()?);
        let mut cells = Vec::with_capacity(count);
        for _ in 0..count {
            let cell = match self.byte()? {
                b'n' => Cell::Null,
                b'u' => Cell::Unchanged,
                kind @ (b't' | b'b') => {
                    let length = usize::try_from(self.u32()?)
                        .map_err(|_| MessageError("cell too long".to_owned()))?;
                    let bytes = self.take(length)?;
                    if kind == b't' {
                        Cell::Text(bytes)
                    } else {
                        Cell::Binary(bytes)
                    }
                }
                other => {
                    return Err(MessageError(format!(
                        "unknown cell kind `{}`",
                        char::from(other)
                    )));
                }
            };
            cells.push(cell);
        }
        Ok(cells)
    }

    fn expect(&mut self, wanted: u8) -> Result<(), MessageError> {
        let found = self.byte()?;
        if found == wanted {
            Ok(())
        } else {
            Err(MessageError(format!(
                "expected `{}`, found `{}`",
                char::from(wanted),
                char::from(found)
            )))
        }
    }
}

/// Reads one message.
pub fn parse(bytes: &[u8]) -> Result<Message<'_>, MessageError> {
    let mut reader = Reader { bytes, at: 0 };
    Ok(match reader.byte()? {
        b'R' => {
            let oid = reader.u32()?;
            let namespace = reader.cstring()?;
            let name = reader.cstring()?;
            let _replica_identity = reader.byte()?;
            let count = usize::from(reader.u16()?);
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                let _flags = reader.byte()?;
                let name = reader.cstring()?;
                let type_oid = reader.u32()?;
                let _modifier = reader.u32()?;
                columns.push(RelationColumn { name, type_oid });
            }
            Message::Relation(Relation {
                oid,
                namespace,
                name,
                columns,
            })
        }
        b'I' => {
            let relation = reader.u32()?;
            reader.expect(b'N')?;
            Message::Insert {
                relation,
                new: reader.tuple()?,
            }
        }
        b'U' => {
            let relation = reader.u32()?;
            let old = match reader.byte()? {
                b'K' | b'O' => {
                    let old = reader.tuple()?;
                    reader.expect(b'N')?;
                    Some(old)
                }
                b'N' => None,
                other => {
                    return Err(MessageError(format!(
                        "unknown update part `{}`",
                        char::from(other)
                    )));
                }
            };
            Message::Update {
                relation,
                old,
                new: reader.tuple()?,
            }
        }
        b'D' => {
            let relation = reader.u32()?;
            match reader.byte()? {
                b'K' | b'O' => {}
                other => {
                    return Err(MessageError(format!(
                        "unknown delete part `{}`",
                        char::from(other)
                    )));
                }
            }
            Message::Delete {
                relation,
                old: reader.tuple()?,
            }
        }
        b'T' => Message::Truncate,
        _ => Message::Other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(cells: &[Cell<'_>]) -> Vec<u8> {
        let mut out = u16::try_from(cells.len()).unwrap().to_be_bytes().to_vec();
        for cell in cells {
            match cell {
                Cell::Null => out.push(b'n'),
                Cell::Unchanged => out.push(b'u'),
                Cell::Text(bytes) | Cell::Binary(bytes) => {
                    out.push(if matches!(cell, Cell::Text(_)) { b't' } else { b'b' });
                    out.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_be_bytes());
                    out.extend_from_slice(bytes);
                }
            }
        }
        out
    }

    #[test]
    fn reads_a_relation() {
        let mut bytes = vec![b'R'];
        bytes.extend_from_slice(&16_385u32.to_be_bytes());
        bytes.extend_from_slice(b"public\0orders\0d");
        bytes.extend_from_slice(&2u16.to_be_bytes());
        bytes.extend_from_slice(b"\x01id\0");
        bytes.extend_from_slice(&20u32.to_be_bytes());
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        bytes.extend_from_slice(b"\x00amount\0");
        bytes.extend_from_slice(&1700u32.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        let Message::Relation(relation) = parse(&bytes).unwrap() else {
            panic!("not a relation");
        };
        assert_eq!(relation.oid, 16_385);
        assert_eq!(relation.namespace, "public");
        assert_eq!(relation.name, "orders");
        assert_eq!(
            relation.columns,
            vec![
                RelationColumn {
                    name: "id".to_owned(),
                    type_oid: 20
                },
                RelationColumn {
                    name: "amount".to_owned(),
                    type_oid: 1700
                },
            ]
        );
    }

    #[test]
    fn reads_rows_with_every_kind_of_cell() {
        let cells = [
            Cell::Binary(&[0, 0, 0, 7]),
            Cell::Null,
            Cell::Unchanged,
            Cell::Text(b"x"),
        ];
        let mut insert = vec![b'I'];
        insert.extend_from_slice(&9u32.to_be_bytes());
        insert.push(b'N');
        insert.extend(tuple(&cells));
        assert_eq!(
            parse(&insert).unwrap(),
            Message::Insert {
                relation: 9,
                new: cells.to_vec()
            }
        );

        let mut update = vec![b'U'];
        update.extend_from_slice(&9u32.to_be_bytes());
        update.push(b'K');
        update.extend(tuple(&[Cell::Binary(&[1])]));
        update.push(b'N');
        update.extend(tuple(&[Cell::Binary(&[2])]));
        assert_eq!(
            parse(&update).unwrap(),
            Message::Update {
                relation: 9,
                old: Some(vec![Cell::Binary(&[1])]),
                new: vec![Cell::Binary(&[2])]
            }
        );

        let mut delete = vec![b'D'];
        delete.extend_from_slice(&9u32.to_be_bytes());
        delete.push(b'O');
        delete.extend(tuple(&[Cell::Null]));
        assert_eq!(
            parse(&delete).unwrap(),
            Message::Delete {
                relation: 9,
                old: vec![Cell::Null]
            }
        );
        assert_eq!(parse(b"B").unwrap(), Message::Other);
    }

    #[test]
    fn truncated_messages_are_errors() {
        let mut insert = vec![b'I'];
        insert.extend_from_slice(&9u32.to_be_bytes());
        insert.push(b'N');
        insert.extend(tuple(&[Cell::Binary(&[0, 0, 0, 7])]));
        for cut in 1..insert.len() {
            assert!(parse(&insert[..cut]).is_err(), "cut at {cut}");
        }
        assert!(parse(&[]).is_err());
    }
}
