package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

public class TopicPartitionUpdate {

	private TopicPartitionUpdateAction action;
	private Map<TopicPartition, Long /* offset */> topicPartitionOffsetMap;
	
	public TopicPartitionUpdate(TopicPartitionUpdateAction action, Map<TopicPartition, Long> topicPartitionOffsetMap) {
		this.action = action;
		this.topicPartitionOffsetMap = topicPartitionOffsetMap;
	}

	public TopicPartitionUpdateAction getAction() {
		return action;
	}
	
	public Map<TopicPartition, Long> getTopicPartitionOffsetMap() {
		return topicPartitionOffsetMap;
	}

	@Override
	public String toString() {
		return "TopicPartitionUpdate [action=" + action + ", topicPartitionOffsetMap=" + topicPartitionOffsetMap + "]";
	}
}
