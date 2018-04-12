extern crate clap;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
extern crate rdkafka;
extern crate uuid;
extern crate chrono;

use clap::{App, Arg};
use std::collections::HashMap;
use kafka::read_topic_into_metrics;
use metric::Metrics;

mod kafka;
mod metric;

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

    let mut start_offsets = HashMap::<i32, i64>::new();
    let mut end_offsets = HashMap::<i32, i64>::new();
    let mut partitions = Vec::<i32>::new();
    let topic = matches.value_of("topic").unwrap();
    let bootstrap_server = matches.value_of("bootstrap-server").unwrap();
    let consumer = kafka::create_client(bootstrap_server);
    debug!("Gathering offsets...");
    kafka::get_topic_offsets(&consumer, topic, &mut partitions, &mut start_offsets, &mut end_offsets);
    debug!("Done.");
    let mut mr = Metrics::new(partitions.len() as i32);
    read_topic_into_metrics(topic, &consumer, &mut mr, &partitions, &start_offsets, &end_offsets);
    println!("{:?}", mr);
}
