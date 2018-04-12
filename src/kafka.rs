use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::types::RDKafkaTopicPartitionList;
use std::collections::HashMap;
use uuid::Uuid;
use std::time::Duration;

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
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(LoggingConsumerContext)
        .expect("Consumer creation failed")
}

pub fn get_topic_offsets(consumer: &LoggingConsumer, topic: &str, parts: &mut Vec<i32>, start_offsets: &mut HashMap<i32, i64>, end_offsets: &mut HashMap<i32, i64>) {
    let md = consumer.fetch_metadata(Option::from(topic), Duration::new(1, 0)).unwrap_or_else(|e| {panic!("Error fetching metadata: {}", e)});
    let topic_metadata = md.topics().first().unwrap_or_else(|| { panic!("Topic not found!") });

    for partition in topic_metadata.partitions() {
        parts.push(partition.id());
        let (low, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::new(1, 0)).unwrap();
        start_offsets.insert(partition.id(), low);
        end_offsets.insert(partition.id(), high);
    }
}
