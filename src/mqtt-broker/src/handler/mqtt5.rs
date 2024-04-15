use super::{packet::MQTTAckBuild, session::save_connect_session, subscribe::send_retain_message};
use crate::{
    cluster::heartbeat_manager::{ConnectionLiveTime, HeartbeatManager},
    metadata::{
        cache::MetadataCache, cluster::Cluster, message::Message, session::LastWillData,
        subscriber::Subscriber, topic::Topic,
    },
    server::tcp::packet::ResponsePackage,
    storage::{message::MessageStorage, topic::TopicStorage},
    subscribe::manager::SubScribeManager,
};
use common_base::{
    log::error,
    tools::{now_second, unique_id_string},
};
use protocol::mqtt::{
    Connect, ConnectProperties, Disconnect, DisconnectProperties, DisconnectReasonCode, LastWill,
    LastWillProperties, Login, MQTTPacket, PingReq, PubAck, PubAckProperties, Publish,
    PublishProperties, Subscribe, SubscribeProperties, Unsubscribe, UnsubscribeProperties,
};
use std::sync::Arc;
use storage_adapter::memory::MemoryStorageAdapter;
use tokio::sync::{broadcast::Sender, RwLock};

#[derive(Clone)]
pub struct Mqtt5Service {
    metadata_cache: Arc<RwLock<MetadataCache>>,
    subscribe_manager: Arc<RwLock<SubScribeManager>>,
    ack_build: MQTTAckBuild,
    heartbeat_manager: Arc<RwLock<HeartbeatManager>>,
    storage_adapter: Arc<MemoryStorageAdapter>,
}

impl Mqtt5Service {
    pub fn new(
        metadata_cache: Arc<RwLock<MetadataCache>>,
        subscribe_manager: Arc<RwLock<SubScribeManager>>,
        ack_build: MQTTAckBuild,
        heartbeat_manager: Arc<RwLock<HeartbeatManager>>,
        storage_adapter: Arc<MemoryStorageAdapter>,
    ) -> Self {
        return Mqtt5Service {
            metadata_cache,
            subscribe_manager,
            ack_build,
            heartbeat_manager,
            storage_adapter,
        };
    }

    pub async fn connect(
        &mut self,
        connect_id: u64,
        connnect: Connect,
        connect_properties: Option<ConnectProperties>,
        last_will: Option<LastWill>,
        last_will_properties: Option<LastWillProperties>,
        login: Option<Login>,
    ) -> MQTTPacket {
        let cache = self.metadata_cache.read().await;
        let cluster = cache.cluster_info.clone();
        drop(cache);

        // connect for authentication
        if self.authentication(login, &cluster).await {
            return self
                .ack_build
                .distinct(DisconnectReasonCode::NotAuthorized, None);
        }

        // auto create client id
        let mut auto_client_id = false;
        let mut client_id = connnect.client_id.clone();
        if client_id.is_empty() {
            client_id = unique_id_string();
            auto_client_id = true;
        }

        // save session data
        let client_session = match save_connect_session(
            auto_client_id,
            client_id.clone(),
            !last_will.is_none(),
            cluster.clone(),
            connnect.clone(),
            connect_properties.clone(),
            self.storage_adapter.clone(),
        )
        .await
        {
            Ok(session) => session,
            Err(e) => {
                error(e.to_string());
                return self.ack_build.distinct(
                    DisconnectReasonCode::AdministrativeAction,
                    Some(e.to_string()),
                );
            }
        };

        // save last will data
        if !last_will.is_none() {
            let last_will = LastWillData {
                last_will,
                last_will_properties,
            };

            let message_storage = MessageStorage::new(self.storage_adapter.clone());
            match message_storage
                .save_lastwill(client_id.clone(), last_will)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error(e.to_string());
                    return self
                        .ack_build
                        .distinct(DisconnectReasonCode::UnspecifiedError, Some(e.to_string()));
                }
            }
        }

        // update cache
        // When working with locks, the default is to release them as soon as possible
        let mut cache = self.metadata_cache.write().await;
        cache.set_session(client_id.clone(), client_session.clone());
        cache.set_client_id(connect_id, client_id.clone());
        drop(cache);

        // Record heartbeat information
        let session_keep_alive = client_session.keep_alive;
        let live_time = ConnectionLiveTime {
            protobol: crate::server::MQTTProtocol::MQTT5,
            keep_live: session_keep_alive,
            heartbeat: now_second(),
        };
        let mut heartbeat = self.heartbeat_manager.write().await;
        heartbeat.report_hearbeat(connect_id, live_time);
        drop(heartbeat);

        let mut user_properties = Vec::new();
        if let Some(connect_properties) = connect_properties {
            user_properties = connect_properties.user_properties;
        }

        let reason_string = Some("".to_string());
        let response_information = Some("".to_string());
        let server_reference = Some("".to_string());
        return self
            .ack_build
            .conn_ack(
                client_id,
                auto_client_id,
                reason_string,
                user_properties,
                response_information,
                server_reference,
                session_keep_alive,
            )
            .await;
    }

    pub async fn publish(
        &self,
        publish: Publish,
        publish_properties: Option<PublishProperties>,
    ) -> MQTTPacket {
        let topic_name = String::from_utf8(publish.topic.to_vec()).unwrap();
        let mut cache = self.metadata_cache.write().await;

        let topic = if let Some(tp) = cache.get_topic_by_name(topic_name.clone()) {
            tp
        } else {
            let topic = Topic::new(&topic_name);
            cache.set_topic(&topic_name, &topic);
            let topic_storage = TopicStorage::new(self.storage_adapter.clone());
            match topic_storage.save_topic(&topic_name, &topic).await {
                Ok(_) => {}
                Err(e) => {
                    error(e.to_string());
                    return self
                        .ack_build
                        .distinct(DisconnectReasonCode::UnspecifiedError, Some(e.to_string()));
                }
            }
            topic
        };

        drop(cache);

        // Persisting retain message data
        let message_storage = MessageStorage::new(self.storage_adapter.clone());
        if publish.retain {
            let retain_message =
                Message::build_message(publish.clone(), publish_properties.clone());
            match message_storage
                .save_retain_message(topic.topic_id.clone(), retain_message)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error(e.to_string());
                    return self
                        .ack_build
                        .distinct(DisconnectReasonCode::UnspecifiedError, Some(e.to_string()));
                }
            }
        }

        // Persisting stores message data
        let mut offset = 0;
        if let Some(record) = Message::build_record(publish.clone(), publish_properties.clone()) {
            match message_storage
                .append_topic_message(topic.topic_id, record)
                .await
            {
                Ok(da) => {
                    offset = da;
                }
                Err(e) => {
                    error(e.to_string());
                    return self
                        .ack_build
                        .distinct(DisconnectReasonCode::UnspecifiedError, Some(e.to_string()));
                }
            }
        }

        // Pub Ack information is built
        let pkid = publish.pkid;
        let mut user_properties = Vec::new();
        if let Some(properties) = publish_properties {
            user_properties = properties.user_properties;
        }
        if offset > 0 {
            user_properties.push(("offset".to_string(), offset.to_string()));
        }

        return self.ack_build.pub_ack(pkid, None, user_properties);
    }

    pub fn publish_ack(&self, pub_ack: PubAck, puback_properties: Option<PubAckProperties>) {}

    pub async fn subscribe(
        &self,
        connect_id: u64,
        subscribe: Subscribe,
        subscribe_properties: Option<SubscribeProperties>,
        response_queue_sx: Sender<ResponsePackage>,
    ) -> MQTTPacket {
        let subscriber = Subscriber::build_subscriber(
            connect_id,
            subscribe.clone(),
            subscribe_properties.clone(),
        );

        // Reservation messages are processed when a subscription is created
        let message_storage = MessageStorage::new(self.storage_adapter.clone());
        match send_retain_message(
            connect_id,
            subscribe.clone(),
            subscribe_properties.clone(),
            message_storage,
            self.metadata_cache.clone(),
            response_queue_sx.clone(),
            true,
            false,
        )
        .await
        {
            Ok(()) => {}
            Err(e) => {
                error(e.to_string());
                return self
                    .ack_build
                    .distinct(DisconnectReasonCode::UnspecifiedError, Some(e.to_string()));
            }
        }

        // Saving subscriptions
        let mut sub_manager = self.subscribe_manager.write().await;
        sub_manager.add_subscribe(connect_id, subscriber);
        drop(sub_manager);

        let pkid = subscribe.packet_identifier;
        let mut user_properties = Vec::new();
        if let Some(properties) = subscribe_properties {
            user_properties = properties.user_properties;
        }
        return self.ack_build.sub_ack(pkid, None, user_properties);
    }

    pub async fn ping(&self, connect_id: u64, _: PingReq) -> MQTTPacket {
        let cache = self.metadata_cache.read().await;
        if let Some(client_id) = cache.connect_id_info.get(&connect_id) {
            if let Some(session_info) = cache.session_info.get(client_id) {
                let live_time = ConnectionLiveTime {
                    protobol: crate::server::MQTTProtocol::MQTT5,
                    keep_live: session_info.keep_alive,
                    heartbeat: now_second(),
                };
                let mut heartbeat = self.heartbeat_manager.write().await;
                heartbeat.report_hearbeat(connect_id, live_time);
                return self.ack_build.ping_resp();
            }
        }
        return self
            .ack_build
            .distinct(DisconnectReasonCode::UseAnotherServer, None);
    }

    pub async fn un_subscribe(
        &self,
        connect_id: u64,
        un_subscribe: Unsubscribe,
        un_subscribe_properties: Option<UnsubscribeProperties>,
    ) -> MQTTPacket {
        // Remove subscription information
        let mut sub_manager = self.subscribe_manager.write().await;
        sub_manager.remove_subscribe(connect_id, Some(un_subscribe.clone()));
        drop(sub_manager);

        let pkid = un_subscribe.pkid;
        let mut user_properties = Vec::new();
        if let Some(properties) = un_subscribe_properties {
            user_properties = properties.user_properties;
        }
        return self.ack_build.unsub_ack(pkid, None, user_properties);
    }

    pub async fn disconnect(
        &self,
        connect_id: u64,
        disconnect: Disconnect,
        disconnect_properties: Option<DisconnectProperties>,
    ) -> MQTTPacket {
        let mut cache = self.metadata_cache.write().await;
        cache.remove_connect_id(connect_id);
        drop(cache);

        let mut heartbeat = self.heartbeat_manager.write().await;
        heartbeat.remove_connect(connect_id);
        drop(heartbeat);

        let mut sub_manager = self.subscribe_manager.write().await;
        sub_manager.remove_subscribe(connect_id, None);
        drop(sub_manager);

        return self
            .ack_build
            .distinct(DisconnectReasonCode::NormalDisconnection, None);
    }

    async fn authentication(&self, login: Option<Login>, cluster: &Cluster) -> bool {
        if cluster.secret_free_login() {
            return true;
        }
        if login == None {
            return false;
        }
        let login_info = login.unwrap();
        let cache = self.metadata_cache.read().await;
        if let Some(user) = cache.user_info.get(&login_info.username) {
            return user.password == login_info.password;
        }
        return false;
    }
}