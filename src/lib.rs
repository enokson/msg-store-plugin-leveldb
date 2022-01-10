use bincode::{serialize, deserialize, deserialize_from};
use msg_store::uuid::Uuid;
use msg_store_db_plugin::Db;
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
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use std::{
    fs::create_dir_all,
    path::Path,
    marker::PhantomData
};

// pub type LevelStore = Store<Leveldb>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct Id {
    pub priority: u32,
    pub timestamp: u128,
    pub sequence: u32
}
impl Id {
    pub fn to_string(&self) -> String {
        format!("{}-{}-{}", self.priority, self.timestamp, self.sequence)
    }
    pub fn from_string(id: &str) -> Uuid {
        Uuid::from_string(id).unwrap()
        // let split_str = id.split("-").collect::<Vec<&str>>();
        // Uuid { 
        //     timestamp: split_str[0].parse().expect("Could not parse timestamp"), 
        //     sequence: split_str[0].parse().expect("Could not parse sequence")
        // }
    }
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self {
            priority: uuid.priority,
            timestamp: uuid.timestamp,
            sequence: uuid.sequence
        }
    }
    pub fn to_uuid(self) -> Uuid {
        Uuid {
            priority: self. priority,
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

pub struct Leveldb<T> {
    pub msgs: Database<Id>,
    pub data: Database<Id>,
    pub msg_type: PhantomData<T>
}

impl<T> Leveldb<T> {
    pub fn new(dir: &Path) -> Result<Leveldb<T>, String> {
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

        let msgs = match Database::open(msgs_path, msgs_options) {
            Ok(db) => Ok(db),
            Err(error) => Err(error.to_string())
        }?;
        let data = match Database::open(Path::new(msg_data_path), msg_data_options) {
            Ok(db) => Ok(db),
            Err(error) => Err(error.to_string())
        }?;
        
        Ok(Leveldb {
            msgs,
            data,
            msg_type: PhantomData
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct DbMetadata {
    priority: u32,
    byte_size: u32
}

impl<'a, T: Serialize + DeserializeOwned> Db<T> for Leveldb<T> {
    fn add(&mut self, uuid: Uuid, msg: T, msg_byte_size: u32) -> Result<(), String> {
        let serialized_data = match serialize(&msg_byte_size) {
            Ok(data) => Ok(data),
            Err(error) => Err(error.to_string())
        }?;
        let msg = match serialize(&msg) {
            Ok(data) => Ok(data),
            Err(error) => Err(error.to_string())
        }?;
        match self.data.put(WriteOptions::new(), Id::from_uuid(uuid), &serialized_data) {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string())
        }?;
        match self.msgs.put(WriteOptions::new(), Id::from_uuid(uuid), &msg) {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string())
        }?;
        Ok(())
    }
    fn get(&mut self, uuid: Uuid) -> Result<T, String> {
        let data = match self.msgs.get(ReadOptions::new(), Id::from_uuid(uuid)) {
            Ok(data) => match data {
                Some(data) => data,
                None => { return Err("msg not found".to_string()) }
            },
            Err(error) => return Err(error.to_string())
        };

        let deserialized_data = match deserialize_from(&*data) {
            Ok(deserialized_data) => Ok(deserialized_data),
            Err(error) => Err(error.to_string())
        };

        deserialized_data  
    }
    fn del(&mut self, uuid: Uuid) -> Result<(), String> {
        match self.msgs.delete(WriteOptions::new(), Id::from_uuid(uuid)) {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string())
        }
    }
    fn fetch(&mut self) -> Result<Vec<(Uuid, u32)>, String> {
        self.data.iter(ReadOptions::new()).map(|(id, data)| -> Result<(Uuid, u32), String> {
            let msg_byte_size: u32 = match deserialize(&data) {
                Ok(data) => Ok(data),
                Err(error) => Err(error.to_string())
            }?;
            Ok((id.to_uuid(), msg_byte_size))
        }).collect::<Result<Vec<(Uuid, u32)>, String>>()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
