use futures::Stream;
use metric::Metrics;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::types::RDKafkaTopicPartitionList;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;
use chrono::Utc;
use rdkafka::message::Message;
use chrono::prelude::*;
use tokio_core::reactor::Core;

pub struct LoggingConsumerContext;

pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: *mut RDKafkaTopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

pub fn create_client(bootstrap_server: &str) -> LoggingConsumer {
    ClientConfig::new()
        .set("group.id", format!("topic-analyzer--{}-{}", env!("USER"), Uuid::new_v4()).as_str())
        .set("bootstrap.servers", bootstrap_server)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("client.id", "topic-analyzer")
        .set("compression.codec", "lz4")
        .set("queue.buffering.max.ms", "100")
        .set("batch.num.messages", "5000")
        .set("socket.nagle.disable", "true")
        .set("socket.keepalive.enable", "true")
        .set("fetch.wait.max.ms", "300")

        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(LoggingConsumerContext)
        .expect("Consumer creation failed")
}

pub fn get_topic_offsets(consumer: &LoggingConsumer, topic: &str, parts: &mut Vec<i32>, start_offsets: &mut HashMap<i32, i64>, end_offsets: &mut HashMap<i32, i64>) {
    let md = consumer.fetch_metadata(Option::from(topic), Duration::new(10, 0)).unwrap_or_else(|e| { panic!("Error fetching metadata: {}", e) });
    let topic_metadata = md.topics().first().unwrap_or_else(|| { panic!("Topic not found!") });

    for partition in topic_metadata.partitions() {
        parts.push(partition.id());
        let (low, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::new(1, 0)).unwrap();
        start_offsets.insert(partition.id(), low);
        end_offsets.insert(partition.id(), high);
    }
}

pub fn read_topic_into_metrics(topic: &str,
                               consumer: &LoggingConsumer,
                               metrics: &mut Metrics,
                               partitions: &[i32],
                               end_offsets: &HashMap<i32, i64>) {
    info!("Subscribing to {}", topic);
    consumer.subscribe(&[topic]).expect("Can't subscribe to specified topic");
    let message_stream = consumer.start();
    info!("Starting message consumption...");

    let mut seq: u64 = 0;

    let mut still_running = HashMap::<i32, bool>::new();
    for &p in partitions {
        still_running.insert(p, true);
    }

    for message in message_stream.wait() {
        match message {
            Err(()) => {
                warn!("Error while reading from stream");
            }
            Ok(Err(e)) => {
                warn!("Kafka error: {}", e);
            }
            Ok(Ok(m)) => {
                seq += 1;
                let partition = m.partition();
                let offset = m.offset();
                let timestamp = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(m.timestamp().to_millis().unwrap() / 1000, 0), Utc);
                let mut message_size: u64 = 0;
                let mut empty_key = false;
                let mut empty_value = false;

                metrics.inc_overall_count();
                metrics.inc_total(partition);

                match m.key() {
                    Some(k) => {
                        metrics.inc_key_non_null(partition);
                        let k_len = k.len() as u64;
                        message_size += k_len;
                        metrics.inc_key_size_sum(partition, k_len);
                        metrics.inc_overall_size(k_len);
                    }
                    None => {
                        empty_key = true;
                        metrics.inc_key_null(partition);
                    }
                }

                match m.payload() {
                    Some(v) => {
                        let v_len = v.len() as u64;
                        message_size += v_len;
                        metrics.inc_value_size_sum(partition, v_len);
                        metrics.inc_overall_size(v_len);
                        metrics.inc_alive(partition);
                    }
                    None => {
                        empty_value = true;
                        metrics.inc_tombstones(partition);
                    }
                }

                metrics.cmp_and_set_message_timestamp(timestamp);

                if !empty_key && !empty_value {
                    metrics.cmp_and_set_message_size(message_size);
                }

                if seq % 50000 == 0 {
                    info!("[Sq: {} | T: {} | P: {} | O: {} | Ts: {}]",
                          seq, topic, partition, offset, timestamp);
                }

                if let Err(e) = consumer.store_offset(&m) {
                    warn!("Error while storing offset: {}", e);
                }

                if (offset + 1) >= *end_offsets.get(&partition).unwrap() {
                    *still_running.get_mut(&partition).unwrap() = false;
                }

                let mut all_done = true;
                for running in still_running.values() {
                    if *running {
                        all_done = false;
                    }
                }

                if all_done {
                    break;
                }
            }
        }
    }
}

pub fn read_topic_into_metrics_async(topic: &str,
                                     consumer: &LoggingConsumer,
                                     metrics: &mut Metrics,
                                     partitions: &[i32],
                                     end_offsets: &HashMap<i32, i64>) {
    info!("Subscribing to {}", topic);
    consumer.subscribe(&[topic]).expect("Can't subscribe to specified topic");
    let message_stream = consumer.start();
    info!("Starting message consumption...");

    let mut core = Core::new().unwrap();
    let mut seq: u64 = 0;

    let mut still_running = HashMap::<i32, bool>::new();
    for &p in partitions {
        still_running.insert(p, true);
    }

    let processed_stream = message_stream
        .filter_map(|result| {  // Filter out errors
            match result {
                Ok(msg) => Some(msg),
                Err(kafka_error) => {
                    warn!("Error while receiving from Kafka: {:?}", kafka_error);
                    None
                }
            }
        })
        .for_each(|m| {
            seq += 1;
            let partition = m.partition();
            let offset = m.offset();
            let timestamp = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(m.timestamp().to_millis().unwrap() / 1000, 0), Utc);
            let mut message_size: u64 = 0;
            let mut empty_key = false;
            let mut empty_value = false;

            metrics.inc_overall_count();
            metrics.inc_total(partition);

            match m.key() {
                Some(k) => {
                    metrics.inc_key_non_null(partition);
                    let k_len = k.len() as u64;
                    message_size += k_len;
                    metrics.inc_key_size_sum(partition, k_len);
                    metrics.inc_overall_size(k_len);
                }
                None => {
                    empty_key = true;
                    metrics.inc_key_null(partition);
                }
            }

            match m.payload() {
                Some(v) => {
                    let v_len = v.len() as u64;
                    message_size += v_len;
                    metrics.inc_value_size_sum(partition, v_len);
                    metrics.inc_overall_size(v_len);
                    metrics.inc_alive(partition);
                }
                None => {
                    empty_value = true;
                    metrics.inc_tombstones(partition);
                }
            }

            metrics.cmp_and_set_message_timestamp(timestamp);

            if !empty_key && !empty_value {
                metrics.cmp_and_set_message_size(message_size);
            }

            if seq % 50000 == 0 {
                info!("[Sq: {} | T: {} | P: {} | O: {} | Ts: {}]",
                      seq, topic, partition, offset, timestamp);
            }

            if let Err(e) = consumer.store_offset(&m) {
                warn!("Error while storing offset: {}", e);
            }

            if (offset + 1) >= *end_offsets.get(&partition).unwrap() {
                *still_running.get_mut(&partition).unwrap() = false;
            }

            let mut all_done = true;
            for running in still_running.values() {
                if *running {
                    all_done = false;
                }
            }

            return if all_done {
                Err(())
            } else {
                Ok(())
            }
        });

    match core.run(processed_stream) {
        Ok(_) => {}
        Err(_) => {}
    };
}
