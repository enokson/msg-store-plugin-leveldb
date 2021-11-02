use bincode::{serialize, deserialize};
use msg_store::{
    Keeper,
    store::{
        Package,
        PacketMetaData,
        Store
    },
    uuid::Uuid
};
use db_key::Key;
use leveldb::{
    database::Database,
    iterator::Iterable,
    kv::KV,
    options::{
        Options,
        ReadOptions,
        WriteOptions
    }
};
use serde::{Serialize, Deserialize};
use std::{
    fs::create_dir_all,
    path::Path
};

pub type LevelStore = Store<Leveldb>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct Id {
    pub timestamp: u128,
    pub sequence: u32
}
impl Id {
    pub fn to_string(&self) -> String {
        format!("{}-{}", self.timestamp, self.sequence)
    }
    pub fn from_string(id: &str) -> Uuid {
        let split_str = id.split("-").collect::<Vec<&str>>();
        Uuid { 
            timestamp: split_str[0].parse().expect("Could not parse timestamp"), 
            sequence: split_str[0].parse().expect("Could not parse sequence")
        }
    }
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self {
            timestamp: uuid.timestamp,
            sequence: uuid.sequence
        }
    }
    pub fn to_uuid(self) -> Uuid {
        Uuid { 
            timestamp: self.timestamp, 
            sequence: self.sequence
        }
    }
}

impl Key for Id {
    fn from_u8(key: &[u8]) -> Self {
        deserialize(key).expect("Could not deserialize key")
    }
    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(&serialize(&self).expect("Could not serialize uuid"))
    }
}

pub fn open(location: &Path) -> LevelStore {
    Store::open(Leveldb::new(location))
}

pub struct Leveldb {
    pub msgs: Database<Id>,
    pub data: Database<Id>
}

impl Leveldb {
    pub fn new(dir: &Path) -> Leveldb {
        create_dir_all(&dir).expect("Could not create db location dir.");

        let mut msgs_path = dir.to_path_buf();
        msgs_path.push("msgs");
        let msgs_path = msgs_path.as_path();

        let mut msg_data_path = dir.to_path_buf();
        msg_data_path.push("msg_data");
        let msg_data_path = msg_data_path.as_path();

        let mut msgs_options = Options::new();
        msgs_options.create_if_missing = true;

        let mut msg_data_options = Options::new();
        msg_data_options.create_if_missing = true;

        let msgs = Database::open(msgs_path, msgs_options).expect("Could not open msgs database");
        let data = Database::open(Path::new(msg_data_path), msg_data_options).expect("Could not open data database");
        
        Leveldb {
            msgs,
            data
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct DbMetadata {
    priority: i32,
    byte_size: i32
}

impl Keeper for Leveldb {
    fn add(&mut self, package: &Package) {
        let data = DbMetadata {
            priority: package.priority,
            byte_size: package.byte_size
        };
        let serialized_data = serialize(&data).expect("Could not serialize data");
        let msg = serialize(&package.msg).expect("Could not serialize msg");
        self.data.put(WriteOptions::new(), Id::from_uuid(package.uuid), &serialized_data).expect("Could not insert metadata");
        self.msgs.put(WriteOptions::new(), Id::from_uuid(package.uuid), &msg).expect("Could not insert msg");     
    }
    fn get(&mut self, uuid: &Uuid) -> Option<String> {
        match self.msgs.get(ReadOptions::new(), Id::from_uuid(*uuid)).expect("Could not get msg") {
            Some(serialized_msg) => Some(deserialize(&serialized_msg).expect("Could not deserialize msg")),
            None => None
        }        
    }
    fn del(&mut self, uuid: &Uuid) {
        self.msgs.delete(WriteOptions::new(), Id::from_uuid(*uuid)).expect("Could not delete msg");
    }
    fn fetch(&mut self) -> Vec<PacketMetaData> {
        self.data.iter(ReadOptions::new()).map(|(id, data)| {
            let db_metadata: DbMetadata = deserialize(&data).expect("Could not deserialize data");
            PacketMetaData { 
                uuid: id.to_uuid(),
                priority: db_metadata.priority, 
                byte_size: db_metadata.byte_size
            }
        }).collect::<Vec<PacketMetaData>>()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
