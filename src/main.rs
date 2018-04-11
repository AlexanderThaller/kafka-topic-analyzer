#[macro_use]
extern crate log;

extern crate env_logger;
extern crate clap;
extern crate rdkafka;
extern crate futures;

use clap::{App, Arg};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::client::ClientContext;
use rdkafka::types::RDKafkaTopicPartitionList;
use futures::stream::Stream;

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
}
