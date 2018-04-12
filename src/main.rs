extern crate clap;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
extern crate rdkafka;
extern crate uuid;

use clap::{App, Arg};
//use futures::stream::Stream;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::types::RDKafkaTopicPartitionList;
use std::collections::HashMap;
use uuid::Uuid;
use rdkafka::metadata::MetadataPartition;
use std::time::Duration;

struct LoggingConsumerContext;

type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: *mut RDKafkaTopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

fn main() {
    env_logger::init();

    let matches = App::new("topic-analyzer")
        .bin_name("topic-analyzer")

        .arg(Arg::with_name("topic")
            .short("t")
            .long("topic")
            .value_name("TOPIC")
            .help("The topic to analyze")
            .takes_value(true)
            .required(true)
        )
        .arg(Arg::with_name("bootstrap-server")
            .short("b")
            .long("bootstrap-server")
            .value_name("BOOTSTRAP_SERVER")
            .help("Bootstrap server(s) to work with, comma separated")
            .takes_value(true)
            .required(true)
        )
        .get_matches();


    let context = LoggingConsumerContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", format!("topic-analyzer--{}-{}", env!("USER"), Uuid::new_v4()).as_str())
        .set("bootstrap.servers", matches.value_of("bootstrap-server").unwrap())
        .set("enable.partition.eof", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");
    let mut start_offsets = HashMap::new();
    let mut end_offsets = HashMap::new();
    let topic_option = matches.value_of("topic");
    let topic = topic_option.unwrap();
    let md = consumer.fetch_metadata(topic_option, Duration::new(1, 0)).unwrap_or_else(|e| {panic!("Error fetching metadata: {}", e)});
    let topic_metadata = md.topics().first().unwrap_or_else(|| { panic!("Topic not found!") });
    let partitions = topic_metadata.partitions();

    for partition in partitions {
        let (low, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::new(1, 0)).unwrap();
        start_offsets.insert(partition.id(), low);
        end_offsets.insert(partition.id(), high);
    }

    println!("{:?}", start_offsets);
    println!("{:?}", end_offsets);
}
