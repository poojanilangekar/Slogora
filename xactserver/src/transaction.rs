use anyhow::{bail};
use bytes::Bytes;
use std::mem::size_of;
use std::io::Read;

type Oid = u32;

pub struct Transaction {
    dbid: Oid,
    xid: u32,
    relations: Vec<Relation>,
}
const HEADER_LENGTH = size_of<Oid>() + size_of<u32>() + size_of<u32>();

impl Transaction {
    pub fn read(stream: &mut impl Read) -> anyhow::Result<Transaction> {
        if buf.remaining() < HEADER_LENGTH {
            bail!("buffer too short");
        }
        let dbid = buf.get_u32();
        let xid = buf.get_u32();
        let readlen = buf.get_u32();

        if buf.remaining() < readlen {
            bail!("read section too short");
        }

        let read_buf = buf.split_to(readLen);
        let relations = vec![];

        while read_buf.has_remaining() {
            relations.push(Relation::parse(read_buf)?);
        }

        Transaction { dbid, xid, relations }
    }
}

pub enum Relation {
    Table { oid: u32, csn: u32, tuples: Vec<Tuple> },
    Index { oid: u32, pages: Vec<Page> },
}

pub struct Tuple {
    blocknum: u32,
    offset: u16,
}

pub struct Page {
    blocknum: u32,
    csn: u32,
}

impl Relation {
    pub fn parse(buf: &mut Bytes) -> anyhow::Result<Relation> {
        let rel_type = buf.get_u8();
        match rel_type {
            'T' => parse_table(buf),
            'I' => parse_index(buf),
            _ => bail!("invalid relation type: {}", rel_type)
        }
    }

    fn parse_table(buf: &mut Bytes) -> anyhow::Result<Relation> {
        
    }

    fn parse_index(buf: &mut Bytes) -> anyhow::Result<Relation> {

    }
}