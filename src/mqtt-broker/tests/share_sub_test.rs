mod common;

#[cfg(test)]
mod tests {
    use crate::common::{broker_addr, connect_server5, distinct_conn};
    use common_base::tools::unique_id;
    use paho_mqtt::{Message, QOS_1};

    #[tokio::test]
    async fn client5_subscribe_test() {
        let sub_qos = &[0];
        let topic = format!("/tests/{}", unique_id());
        let sub_topic = format!("$share/tests/{}", unique_id());
        simple_test(topic.clone(), sub_topic.clone(), sub_qos, "2".to_string()).await;

        // let sub_qos = &[1];
        // let topic = format!("/tests/{}", unique_id());
        // let sub_topic = format!("$share/tests/{}", unique_id());
        // simple_test(topic.clone(), sub_topic.clone(), sub_qos, "1".to_string()).await;

        // let sub_qos = &[2];
        // let topic = format!("/tests/{}", unique_id());
        // let sub_topic = format!("$share/tests/{}", unique_id());
        // simple_test(topic.clone(), sub_topic.clone(), sub_qos, "3".to_string()).await;
    }

    async fn simple_test(
        pub_topic: String,
        sub_topic: String,
        sub_qos: &[i32],
        payload_flag: String,
    ) {
        let client_id = unique_id();
        let addr = broker_addr();
        let sub_topics = &[sub_topic.clone()];

        let cli = connect_server5(&client_id, &addr);
        let message_content = format!("mqtt {payload_flag} message");

        // publish
        let msg = Message::new(pub_topic.clone(), message_content.clone(), QOS_1);
        match cli.publish(msg) {
            Ok(_) => {}
            Err(e) => {
                println!("{}", e);
                assert!(false);
            }
        }

        // subscribe
        let rx = cli.start_consuming();
        match cli.subscribe_many(sub_topics, sub_qos) {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e)
            }
        }
        for msg in rx.iter() {
            if let Some(msg) = msg {
                let payload = String::from_utf8(msg.payload().to_vec()).unwrap();
                if payload == message_content {
                    assert!(true);
                } else {
                    assert!(false);
                }
                break;
            } else {
                assert!(false);
            }
        }
        distinct_conn(cli);
    }
}
