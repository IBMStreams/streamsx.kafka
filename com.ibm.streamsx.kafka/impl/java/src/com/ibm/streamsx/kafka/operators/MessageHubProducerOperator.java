package com.ibm.streamsx.kafka.operators;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.kafka.operators.utils.MessageHubOperatorUtil;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

@PrimitiveOperator(name="MessageHubProducer", namespace="com.ibm.streamsx.kafka.messagehub")
@Icons(location16="icons/MessageHubProducer_16.png", location32="icons/MessageHubProducer_32.png")
public class MessageHubProducerOperator extends KafkaProducerOperator {

	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(MessageHubProducerOperator.class);
	
	private String messageHubCredsFile = "etc/" + MessageHubOperatorUtil.DEFAULT_MESSAGE_HUB_CREDS_FILE_PATH; //$NON-NLS-1$
	
	@Parameter(optional = true)
	public void setMessageHubCredsFile(String messageHubCredsFile) {
		this.messageHubCredsFile = messageHubCredsFile;
	}
	
	@Override
	protected void loadProperties() throws Exception {
		KafkaOperatorProperties messageHubProperties = MessageHubOperatorUtil.loadMessageHubCredsFromFile(getOperatorContext(), convertToAbsolutePath(messageHubCredsFile));
		loadFromProperties(messageHubProperties);

		super.loadProperties();
	}

	@Override
	protected void loadFromAppConfig() throws Exception {
		KafkaOperatorProperties messageHubProperties = MessageHubOperatorUtil.loadMessageHubCredsFromAppConfig(getOperatorContext(), appConfigName);
		Properties properties = new Properties(messageHubProperties);

		// add app config properties EXCEPT the default message hub creds property name
		Map<String, String> appConfigProps = getOperatorContext().getPE().getApplicationConfiguration(appConfigName);
		appConfigProps.forEach((key, value) -> {
			if(!key.equals(MessageHubOperatorUtil.DEFAULT_MESSAGE_HUB_CREDS_PROPERTY_NAME))
				properties.put(key, value);
		});
		loadFromProperties(properties);
	}
}
