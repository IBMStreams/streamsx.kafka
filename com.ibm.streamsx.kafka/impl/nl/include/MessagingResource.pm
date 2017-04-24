# This is a generated module.  Any modifications will be lost.
package MessagingResource;
use strict;
use Cwd qw(abs_path);
use File::Basename;
unshift(@INC, $ENV{STREAMS_INSTALL} . "/system/impl/bin") if ($ENV{STREAMS_INSTALL});
require SPL::Helper;
my $toolkitRoot = dirname(abs_path(__FILE__)) . '/../../..';

sub INITIALCONTEXT_NON_EXISTENT()
{
   my $defaultText = <<'::STOP::';
A value for the InitialContext was not specified.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1400E", \$defaultText, @_);
}


sub CONFAC_LOOKUP_FAILURE()
{
   my $defaultText = <<'::STOP::';
The system cannot look up the Connection Factory.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1401E", \$defaultText, @_);
}


sub DEST_LOOKUP_FAILURE()
{
   my $defaultText = <<'::STOP::';
The system cannot look up the Destination.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1402E", \$defaultText, @_);
}


sub CONFAC_DEST_LOOKUP_FAILURE()
{
   my $defaultText = <<'::STOP::';
The system cannot look up the Connection Factory or the Destination.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1403E", \$defaultText, @_);
}


sub INITIALCONTEXT_CREATION_ERROR($)
{
   my $defaultText = <<'::STOP::';
The InitialContext cannot be created. The exception is: {0}. 
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1404E", \$defaultText, @_);
}


sub ADMINISTERED_OBJECT_ERROR()
{
   my $defaultText = <<'::STOP::';
Exception occurred during the creation of the administered objects.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1405E", \$defaultText, @_);
}


sub CONNECTION_FAILURE_NORETRY()
{
   my $defaultText = <<'::STOP::';
The connection to the IBM Message Service Client failed. The NoRetry value of the reConnection Policy mandates that a reconnection is not allowed.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1406E", \$defaultText, @_);
}


sub SET_DELIVERY_MODE_FAILURE($)
{
   my $defaultText = <<'::STOP::';
The delivery mode cannot be set. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1407E", \$defaultText, @_);
}


sub CREATE_PRODUCER_EXCEPTION($)
{
   my $defaultText = <<'::STOP::';
The Producer cannot be created. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1408E", \$defaultText, @_);
}


sub CREATE_CONSUMER_EXCEPTION($)
{
   my $defaultText = <<'::STOP::';
The Consumer cannot be created. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1409E", \$defaultText, @_);
}


sub CREATE_DESTINATION_EXCEPTION($)
{
   my $defaultText = <<'::STOP::';
The Destination cannot be created. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1410E", \$defaultText, @_);
}


sub CREATE_SESSION_EXCEPTION($)
{
   my $defaultText = <<'::STOP::';
The Session cannot be created. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1411E", \$defaultText, @_);
}


sub XMS_API_OBJECT_ERROR($)
{
   my $defaultText = <<'::STOP::';
An exception occurred during the creation of the IBM Message Service Client Application Programming Interface Objects. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1412E", \$defaultText, @_);
}


sub XMS_API_UNKNOWN_EXCEPTION()
{
   my $defaultText = <<'::STOP::';
An unknown exception occurred during the creation of the IBM Message Service Client Application Programming Interface Objects.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1413E", \$defaultText, @_);
}


sub CREATE_CONNECTION_EXCEPTION($)
{
   my $defaultText = <<'::STOP::';
The Connection cannot be created. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1414E", \$defaultText, @_);
}


sub XMS_JMS_EXCEPTION($)
{
   my $defaultText = <<'::STOP::';
{0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1415E", \$defaultText, @_);
}


sub PREVIOUS_ERROR()
{
   my $defaultText = <<'::STOP::';
The system cannot proceed because of the previous errors.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1416E", \$defaultText, @_);
}


sub MESSAGE_DROPPED()
{
   my $defaultText = <<'::STOP::';
The message was dropped because the system was not connected to the IBM Message Service Client.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1417E", \$defaultText, @_);
}


sub EXCEPTION($$)
{
   my $defaultText = <<'::STOP::';
An exception occurred while the system was sending the message. The exception is: {0}. The error code is: {1}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1418E", \$defaultText, @_);
}


sub STREAMS_EXCEPTION($$)
{
   my $defaultText = <<'::STOP::';
An InfoSphere Streams exception occurred while the system was sending the message. The exception is: {0}. The explanation is: {1}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1419E", \$defaultText, @_);
}


sub OTHER_EXCEPTION($)
{
   my $defaultText = <<'::STOP::';
Other exception occurred while the system was sending the message. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1420E", \$defaultText, @_);
}


sub UNKNOWN_EXCEPTION()
{
   my $defaultText = <<'::STOP::';
An unknown exception occurred while the system was sending the message.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1421E", \$defaultText, @_);
}


sub MESSAGE_LISTENER_ERROR($)
{
   my $defaultText = <<'::STOP::';
The message listener with ID cannot be started. The exception is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1422E", \$defaultText, @_);
}


sub UNABLE_PROCESS_MESSAGE($)
{
   my $defaultText = <<'::STOP::';
The incoming message cannot be processed. The exception is: {0}. 
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1423E", \$defaultText, @_);
}


sub TXT_UNSUPPORTED($)
{
   my $defaultText = <<'::STOP::';
Text messages are not supported. The access specification cannot be used: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1424E", \$defaultText, @_);
}


sub OBJ_UNSUPPORTED($)
{
   my $defaultText = <<'::STOP::';
Object messages are not supported. The access specification cannot be used: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1425E", \$defaultText, @_);
}


sub PROTOCOLTYPE_NON_SSL1()
{
   my $defaultText = <<'::STOP::';
Please set the protocol type to ssl when trustStore is provided.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1444E", \$defaultText, @_);
}


sub TRUSTSTORE_NON_EXISTENT()
{
   my $defaultText = <<'::STOP::';
Trust Store does not exist.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1445E", \$defaultText, @_);
}


sub PROTOCOLTYPE_NON_SSL2()
{
   my $defaultText = <<'::STOP::';
Please set the protocol type to ssl when keyStore is provided.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1446E", \$defaultText, @_);
}


sub KEYSTORE_NON_EXISTENT()
{
   my $defaultText = <<'::STOP::';
Key Store does not exist.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1447E", \$defaultText, @_);
}


sub KS_TS_ABSENT_SSL()
{
   my $defaultText = <<'::STOP::';
KeyStore or trustStore should be provided when ssl protocol is used.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1448E", \$defaultText, @_);
}


sub BAD_RETURN_CODE($$)
{
   my $defaultText = <<'::STOP::';
Bad return code from MQTTClient_publishMessage returncode={0} handlecode={1}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1449E", \$defaultText, @_);
}


sub RETRY_PUBLISH($$)
{
   my $defaultText = <<'::STOP::';
Error occurred while trying to publish message - the return code is {0} and handle code is {1}, retrying to publish.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1450E", \$defaultText, @_);
}


sub CLNT_CREATE_FAILED_NO_RECONNECT()
{
   my $defaultText = <<'::STOP::';
Creation of MQTT client failed. Did not try to reconnect as the policy is noRetry.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1451E", \$defaultText, @_);
}


sub CLNT_CREATE_FAILED_BOUNDED_RETRY($)
{
   my $defaultText = <<'::STOP::';
Creation of MQTT client failed after trying for {0} times. Did not try to reconnect as the policy is BoundedRetry.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1452E", \$defaultText, @_);
}


sub CONN_FAILURE_NO_RECONNECT()
{
   my $defaultText = <<'::STOP::';
Connection to MQTT failed. Did not try to reconnect as the policy is NoRetry. 
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1453E", \$defaultText, @_);
}


sub CONN_FAILURE_BOUNDED_RETRY($)
{
   my $defaultText = <<'::STOP::';
Connection to MQTT failed after retrying for {0} times. Did not try to reconnect as the policy is BoundedRetry.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1454E", \$defaultText, @_);
}


sub CONTEXT_NULL()
{
   my $defaultText = <<'::STOP::';
Context information is null.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1455E", \$defaultText, @_);
}


sub CONN_ESTABLISH_FAILURE()
{
   my $defaultText = <<'::STOP::';
Failed to establish connection, exiting.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1456E", \$defaultText, @_);
}


sub SUBSCRIBE_FAILURE()
{
   my $defaultText = <<'::STOP::';
Failed to subscribe, exiting.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1457E", \$defaultText, @_);
}


sub DISCONNECT_START_FAILED($)
{
   my $defaultText = <<'::STOP::';
Failed to start disconnect, return code: {0}
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1458E", \$defaultText, @_);
}


sub SUBSCRIBE_FAILED()
{
   my $defaultText = <<'::STOP::';
Subscribe failed, will abort the application.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1459E", \$defaultText, @_);
}


sub INVALID_QOS()
{
   my $defaultText = <<'::STOP::';
Invalid QoS value provided, supported values are 0,1,2.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1500E", \$defaultText, @_);
}


sub TOPIC_QOS_SIZE()
{
   my $defaultText = <<'::STOP::';
QoS value should be provided for every topic. 
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1501E", \$defaultText, @_);
}


sub TRUST_SSL()
{
   my $defaultText = <<'::STOP::';
Set the protocol type to ssl when trustStore is provided.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1502E", \$defaultText, @_);
}


sub TRUST_STORE_MISSING()
{
   my $defaultText = <<'::STOP::';
TrustStore file could not be loaded.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1503E", \$defaultText, @_);
}


sub KEY_SSL()
{
   my $defaultText = <<'::STOP::';
Set the protocol type to ssl when keyStore is provided.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1504E", \$defaultText, @_);
}


sub KEY_STORE_MISSING()
{
   my $defaultText = <<'::STOP::';
KeyStore file could not be loaded.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1505E", \$defaultText, @_);
}


sub STORE_SSL()
{
   my $defaultText = <<'::STOP::';
KeyStore or TrustStore should be provided when ssl protocol is being used.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1506E", \$defaultText, @_);
}


sub CONNECT_MQTT_FAIL_ABORT()
{
   my $defaultText = <<'::STOP::';
Connection to MQTT failed, Aborting.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1507E", \$defaultText, @_);
}


sub BAD_RETRY_PUBLISH($$)
{
   my $defaultText = <<'::STOP::';
An error occurred that can not be handled while trying to publish message, with return code {0}, handle code is {1}, exiting.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1508E", \$defaultText, @_);
}


sub PERIOD_NON_NEGATIVE()
{
   my $defaultText = <<'::STOP::';
Period value cannot be negative.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1509E", \$defaultText, @_);
}


sub DISCARD_MSG_WRONG_TYPE($$)
{
   my $defaultText = <<'::STOP::';
The message has the wrong type and is being discarded. The expected message type was {0}, but the a {1} message was received.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1460W", \$defaultText, @_);
}


sub DISCARD_MSG_INVALID_LENGTH($)
{
   my $defaultText = <<'::STOP::';
The message was discarded because the message length of {0} is not valid.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1461W", \$defaultText, @_);
}


sub DISCARD_MSG_TOO_SHORT()
{
   my $defaultText = <<'::STOP::';
The message was discarded because the message is too short.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1462W", \$defaultText, @_);
}


sub DISCARD_MSG_MISSING_BODY($)
{
   my $defaultText = <<'::STOP::';
The message was discarded because the message does not contain a body. A {0} message was expected.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1463W", \$defaultText, @_);
}


sub DISCARD_MSG_UNRECOG_TYPE()
{
   my $defaultText = <<'::STOP::';
The message was discarded because the type is not recognized.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1464W", \$defaultText, @_);
}


sub DISCARD_MSG_MISSING_ATTR()
{
   my $defaultText = <<'::STOP::';
The message was discarded because the at least one attribute is missing.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1465W", \$defaultText, @_);
}


sub INVALID_QOS_WARN()
{
   my $defaultText = <<'::STOP::';
Invalid QoS value provided, supported value is 0,1 or 2. Hence,default value of zero will be used.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1466W", \$defaultText, @_);
}


sub TOPIC_QOS_SIZE_WARN()
{
   my $defaultText = <<'::STOP::';
QoS value should be provided for every topic. Hence, control signal will be ignored.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1467W", \$defaultText, @_);
}


sub STORE_SSL_WARN()
{
   my $defaultText = <<'::STOP::';
KeyStore or TrustStore should be provided when ssl protocol is being used. Hence, supplied configuration cannot be applied.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1468W", \$defaultText, @_);
}


sub KEY_SSL_WARN()
{
   my $defaultText = <<'::STOP::';
Set the protocol type to ssl when keyStore is provided. Hence, supplied configuration is invalid.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1469W", \$defaultText, @_);
}


sub TRUST_SSL_WARN()
{
   my $defaultText = <<'::STOP::';
Set the protocol type to ssl when trustStore is provided.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1470W", \$defaultText, @_);
}


sub SERVER_URI_WARN()
{
   my $defaultText = <<'::STOP::';
MQTTConfig control tuple does not contain new serverURI. Hence, the configuration will be ignored.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1471W", \$defaultText, @_);
}


sub REMOVE_INVALID()
{
   my $defaultText = <<'::STOP::';
Atleast one topic should be provided, cannot remove all. Hence, the configuration will be ignored.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1472W", \$defaultText, @_);
}


sub INVALID_MQTT_CONFIG_VALUE()
{
   my $defaultText = <<'::STOP::';
Invalid configuration key provided for mqttConfig attribute. Supported values are connection.serverURI, connection.keyStore, connection.keyStorePassword , connection.trustStore. Hence control signal will be ignored.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1473W", \$defaultText, @_);
}


sub XMS_CONNECT()
{
   my $defaultText = <<'::STOP::';
The system is about to connect to the IBM Message Service Client.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1520I", \$defaultText, @_);
}


sub CONNECTION_FAILURE_BOUNDEDRETRY($$)
{
   my $defaultText = <<'::STOP::';
The connection to the IBM Message Service Client failed. The system is waiting for {0} seconds before it reconnects. The number of connection attempts is: {1}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1521I", \$defaultText, @_);
}


sub CONNECTION_FAILURE_INFINITERETRY($)
{
   my $defaultText = <<'::STOP::';
The connection to the IBM Message Service Client failed. The system is waiting for {0} seconds before it reconnects.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1522I", \$defaultText, @_);
}


sub CONNECTION_SUCCESSFUL()
{
   my $defaultText = <<'::STOP::';
The connection to the IBM Message Service Client succeeded.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1523I", \$defaultText, @_);
}


sub SEND_TUPLE_ERROR_PORT()
{
   my $defaultText = <<'::STOP::';
The tuple is being sent to the error output port.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1524I", \$defaultText, @_);
}


sub SENT_MESSAGE($)
{
   my $defaultText = <<'::STOP::';
The message was sent. The ID is: {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1525I", \$defaultText, @_);
}


sub INIT_WAIT($)
{
   my $defaultText = <<'::STOP::';
The wait time before starting is {0} seconds.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1526I", \$defaultText, @_);
}


sub MQTT_CLIENT_DISCONNECTED()
{
   my $defaultText = <<'::STOP::';
MQTT Client Disconnected.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1528I", \$defaultText, @_);
}


sub PUBLISH_SUCCESS($)
{
   my $defaultText = <<'::STOP::';
Successfully published message for client {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1529I", \$defaultText, @_);
}


sub RECONNECT_ATTEMPT($)
{
   my $defaultText = <<'::STOP::';
Attempting to connect with clientId {0}.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1530I", \$defaultText, @_);
}


sub MQTT_CLNT_CREATION_FAILED($)
{
   my $defaultText = <<'::STOP::';
Failed to create MQTT Client instance, with return code {0} , retrying.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1531I", \$defaultText, @_);
}


sub RETRY_CONNECT()
{
   my $defaultText = <<'::STOP::';
Connection to MQTT failed, Trying to reconnect.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1532I", \$defaultText, @_);
}


sub CONN_NOT_SUCCESS()
{
   my $defaultText = <<'::STOP::';
Connection is unsuccessfull. 
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1533I", \$defaultText, @_);
}


sub CONN_SUCCESS()
{
   my $defaultText = <<'::STOP::';
Connection is successful.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1534I", \$defaultText, @_);
}


sub CONN_ESTABLISH_FAIL()
{
   my $defaultText = <<'::STOP::';
Failed to establish connection.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1535I", \$defaultText, @_);
}


sub MSG_RECEIVED($)
{
   my $defaultText = <<'::STOP::';
Message received on topic {0} .
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1536I", \$defaultText, @_);
}


sub SUBSCRIBE_SUCCESS()
{
   my $defaultText = <<'::STOP::';
Subscribe succeeded, waiting to receive messages.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1537I", \$defaultText, @_);
}


sub INTERMITTENT_FAIL()
{
   my $defaultText = <<'::STOP::';
Connection failed in between, will try to re-connect.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1538I", \$defaultText, @_);
}


sub INVALID_QOS_INFO_TUPLE()
{
   my $defaultText = <<'::STOP::';
Invalid QoS value provided in the incoming tuple, supported value is 0,1 or 2. Hence, default value of Qos=0 is used.
::STOP::
    return SPL::Helper::SPLFormattedMessage($toolkitRoot, "com.ibm.streamsx.kafka", "MessagingResource", "en_US/MessagingResource.xlf", "CDIST1539I", \$defaultText, @_);
}

1;
