package com.ibm.streamsx.kafka.operators;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.kafka.operators.utils.MessageHubOperatorUtil;

@PrimitiveOperator(name = "MessageHubConsumer", namespace = "com.ibm.streamsx.kafka.messagehub")
@Icons(location16 = "icons/MessageHubConsumer_16.png", location32 = "icons/MessageHubConsumer_32.png")
public class MessageHubConsumerOperator extends KafkaConsumerOperator {
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(MessageHubConsumerOperator.class);

    private String messageHubCredsFile = MessageHubOperatorUtil.DEFAULT_MESSAGE_HUB_CREDS_FILE_PATH;

    @Parameter(optional = true)
    public void setMessageHubCredsFile(String messageHubCredsFile) {
        this.messageHubCredsFile = messageHubCredsFile;
    }

    @Override
    protected void loadProperties() throws Exception {
        getKafkaProperties().putAllIfNotPresent(MessageHubOperatorUtil.loadMessageHubCredsFromFile(getOperatorContext(),
                convertToAbsolutePath(messageHubCredsFile)));
        super.loadProperties();
    }

    @Override
    protected void loadFromAppConfig() throws Exception {
        getKafkaProperties().putAllIfNotPresent(
                MessageHubOperatorUtil.loadMessageHubCredsFromAppConfig(getOperatorContext(), appConfigName));
        super.loadFromAppConfig();
    }
}
