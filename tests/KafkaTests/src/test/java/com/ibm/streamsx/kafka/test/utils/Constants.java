package com.ibm.streamsx.kafka.test.utils;

public interface Constants {

    public static final String TOPIC_TEST = "test";
    public static final String TOPIC_OTHER1 = "other1";
    public static final String TOPIC_OTHER2 = "other2";
    public static final String TOPIC_POS = "position";
    public static final String APP_CONFIG = "kafka-test";

    public static final String KafkaProducerOp = "com.ibm.streamsx.kafka::KafkaProducer";
    public static final String KafkaConsumerOp = "com.ibm.streamsx.kafka::KafkaConsumer";
    public static final String MessageHubConsumerOp = "com.ibm.streamsx.kafka.messagehub::MessageHubConsumer";
    public static final String MessageHubProducerOp = "com.ibm.streamsx.kafka.messagehub::MessageHubProducer";	

    public static final Long PRODUCER_DELAY = 5000l;

    public static final String[] STRING_DATA = {"dog", "cat", "bird", "fish", "lion", "wolf", "elephant", "zebra", "monkey", "bat"};
    public static final String PROPERTIES_FILE_PATH = "etc/brokers.properties";
}
