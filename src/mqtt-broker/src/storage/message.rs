use super::keys::{lastwill_key, retain_message};
use crate::metadata::{message::Message as RetainMessage, session::LastWillData};
use common_base::errors::RobustMQError;
use std::sync::Arc;
use storage_adapter::{memory::MemoryStorageAdapter, record::Record, storage::StorageAdapter};

#[derive(Clone)]
pub struct MessageStorage {
    storage_adapter: Arc<MemoryStorageAdapter>,
}

impl MessageStorage {
    pub fn new(storage_adapter: Arc<MemoryStorageAdapter>) -> Self {
        return MessageStorage { storage_adapter };
    }

    // Save the data for the Topic dimension
    pub async fn append_topic_message(
        &self,
        topic_id: String,
        record: Record,
    ) -> Result<usize, RobustMQError> {
        let shard_name = topic_id;
        match self.storage_adapter.stream_write(shard_name, record).await {
            Ok(id) => {
                return Ok(id);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    // Read the data for the Topic dimension
    pub async fn read_topic_message(
        &self,
        topic_id: String,
        group_id: String,
        record_num: usize,
    ) -> Result<Vec<Record>, RobustMQError> {
        let shard_name = topic_id;
        match self
            .storage_adapter
            .stream_read(shard_name, group_id, Some(record_num), None)
            .await
        {
            Ok(Some(data)) => {
                return Ok(data);
            }
            Ok(None) => {
                return Ok(Vec::new());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    // Saves the most recent reserved message for the Topic dimension
    pub async fn save_retain_message(
        &self,
        topic_id: String,
        retail_message: RetainMessage,
    ) -> Result<(), RobustMQError> {
        let key = retain_message(topic_id);
        match serde_json::to_vec(&retail_message) {
            Ok(data) => return self.storage_adapter.set(key, Record::build_b(data)).await,
            Err(e) => {
                return Err(common_base::errors::RobustMQError::CommmonError(
                    e.to_string(),
                ))
            }
        }
    }

    // Get the latest reserved message for the Topic dimension
    pub async fn get_retain_message(
        &self,
        topic_id: String,
    ) -> Result<Option<RetainMessage>, RobustMQError> {
        let key = retain_message(topic_id);
        match self.storage_adapter.get(key).await {
            Ok(Some(data)) => match serde_json::from_slice(&data.data) {
                Ok(da) => {
                    return Ok(da);
                }
                Err(e) => {
                    return Err(common_base::errors::RobustMQError::CommmonError(
                        e.to_string(),
                    ))
                }
            },
            Ok(None) => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    // Persistence holds the will message of the connection dimension
    pub async fn save_lastwill(
        &self,
        client_id: String,
        last_will_data: LastWillData,
    ) -> Result<(), RobustMQError> {
        let key = lastwill_key(client_id);
        match serde_json::to_vec(&last_will_data) {
            Ok(data) => return self.storage_adapter.set(key, Record::build_b(data)).await,
            Err(e) => {
                return Err(common_base::errors::RobustMQError::CommmonError(
                    e.to_string(),
                ))
            }
        }
    }

    // Get the will message of the connection dimension
    pub async fn get_lastwill(
        &self,
        client_id: String,
    ) -> Result<Option<LastWillData>, RobustMQError> {
        let key = lastwill_key(client_id);
        match self.storage_adapter.get(key).await {
            Ok(Some(data)) => match serde_json::from_slice(&data.data) {
                Ok(da) => {
                    return Ok(da);
                }
                Err(e) => {
                    return Err(common_base::errors::RobustMQError::CommmonError(
                        e.to_string(),
                    ))
                }
            },
            Ok(None) => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}