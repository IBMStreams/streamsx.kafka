package com.ibm.streamsx.kafka.clients.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallback implements Callback {

    private KafkaProducerClient client;

    public ProducerCallback(KafkaProducerClient client) {
        this.client = client;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
//            exception.printStackTrace();
            client.handleSendException (exception);
        }
    }
}
