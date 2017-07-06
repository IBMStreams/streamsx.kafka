package com.ibm.streamsx.kafka.samples.partitioner;

import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class RandomPartitioner implements Partitioner {

	private Random random;
	
	public RandomPartitioner() {
		random = new Random();
	}
	
	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		Integer numPartitions = cluster.partitionCountForTopic(topic);
		int partition = random.nextInt(numPartitions);
		System.out.println("Assigning to partition=" + partition);
		return partition;
	}

	@Override
	public void close() {
	}

}
