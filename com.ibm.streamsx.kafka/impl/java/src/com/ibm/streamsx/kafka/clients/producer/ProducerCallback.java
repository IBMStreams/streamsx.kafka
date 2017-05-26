package com.ibm.streamsx.kafka.clients.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class ProducerCallback implements Callback {

	private static final Logger logger = Logger.getLogger(ProducerCallback.class);
	private KafkaProducerClient client;
	
	public ProducerCallback(KafkaProducerClient client) {
		this.client = client;
	}
	
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception != null) {
			logger.error(exception.getLocalizedMessage(), exception);
			exception.printStackTrace();
			client.setSendException(exception);
		}
	}
	
}
