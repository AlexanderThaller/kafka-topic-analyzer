use std::collections::HashMap;
use chrono::prelude::*;

type Partition = i32;
type PartitionedCounterBucket = HashMap<Partition, u64>;
type MetricRegistry = HashMap<String, PartitionedCounterBucket>;

enum MetricNames {
    TopicMessagesTotal,
    TopicMessagesTombstones,
    TopicMessagesAlive,
    TopicMessagesKeyNull,
    TopicMessagesKeyNonNull,
    TopicMessagesKeySizeSum,
    TopicMessagesValueSizeSum,
}

pub struct Metrics {
    registry: MetricRegistry,
    earliest_message: DateTime<Utc>,
    latest_message: DateTime<Utc>,
    smallest_message: u64,
    largest_message: u64,
    overall_size: u64
}

impl Metrics {
    fn new(number_of_partitions: i32) -> Metrics {
        let mut mr = MetricRegistry::new();
        let keys = vec![
            "topic.messages.total",
            "topic.messages.tombstones",
            "topic.messages.alive",
            "topic.messages.key.null",
            "topic.messages.key.non-null",
            "topic.messages.key-size.sum",
            "topic.messages.value-size.sum"];
        for key in keys {
            let mut pcb = PartitionedCounterBucket::new();
            for i in 0..number_of_partitions {
                pcb.insert(i, 0u64);
            }
            mr.insert(String::from(key), pcb);
        }
        Metrics {
            registry: mr,
            earliest_message: Utc::now(),
            latest_message: Utc::now(),
            largest_message: 0,
            smallest_message: <u64>::max_value(),
            overall_size: 0
        }
    }

    pub fn inc_overall_size(&mut self, amount: u64) {
        self.overall_size += amount;
    }

    pub fn cmp_and_set_largest_message(&mut self, size: u64) {
        if self.largest_message < size {
            self.largest_message = size;
        }
    }

    pub fn cmp_and_set_smallest_message(&mut self, size: u64) {
        if self.smallest_message > size {
            self.smallest_message = size;
        }
    }

    pub fn cmp_and_set_earliest_message(&mut self, cmp: DateTime<Utc>) {
        if self.earliest_message.gt(&cmp) {
            self.earliest_message = cmp;
        }
    }

    pub fn cmp_and_set_latest_message(&mut self, cmp: DateTime<Utc>) {
        if self.latest_message.lt(&cmp) {
            self.latest_message= cmp;
        }
    }

    pub fn inc_total(&mut self, p: Partition) {
        self.increment("topic.messages.total", p);
    }

    pub fn inc_tombstones(&mut self, p: Partition) {
        self.increment("topic.messages.tombstones", p);
    }

    pub fn inc_alive(&mut self, p: Partition) {
        self.increment("topic.messages.alive", p);
    }

    pub fn inc_key_null(&mut self, p: Partition) {
        self.increment("topic.messages.key.null", p);
    }

    pub fn inc_key_non_null(&mut self, p: Partition) {
        self.increment("topic.messages.key.non-null", p);
    }

    pub fn inc_key_size_sum(&mut self, p: Partition) {
        self.increment("topic.messages.key-size.sum", p);
    }

    pub fn inc_value_size_sum(&mut self, p: Partition) {
        self.increment("topic.messages.value-size.sum", p);
    }

    fn increment(&mut self, key: &str, p: Partition) {
        *self.registry.get_mut(key).unwrap().get_mut(&p).unwrap() += 1;
    }
}

#[test]
fn test_metrics() {
    let mut mr = Metrics::new(10);
    mr.inc_total(0);
    mr.inc_total(1);
    mr.inc_total(1);
    assert_eq!(*mr.registry.get("topic.messages.total").unwrap().get(&0).unwrap(), 1u64);
    assert_eq!(*mr.registry.get("topic.messages.total").unwrap().get(&1).unwrap(), 2u64);
}
