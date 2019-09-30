/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.streamsx.kafka;

public interface IGovernanceConstants {
    public static final String TAG_OPERATOR_IGC = "OperatorIGC"; //$NON-NLS-1$
    public static final String TAG_REGISTER_TYPE = "registerType"; //$NON-NLS-1$
    public static final String TAG_REGISTER_TYPE_INPUT = "input"; //$NON-NLS-1$
    public static final String TAG_REGISTER_TYPE_OUTPUT = "output"; //$NON-NLS-1$
    
    public static final String ASSET_STREAMS_PREFIX = "$Streams-"; //$NON-NLS-1$
    
    public static final String ASSET_JMS_SERVER_TYPE = "JMSServer"; //$NON-NLS-1$
    
    public static final String ASSET_JMS_MESSAGE_TYPE = "JMS"; //$NON-NLS-1$

    public static final String ASSET_KAFKA_TOPIC_TYPE = "KafkaTopic"; //$NON-NLS-1$
    
    public static final String ASSET_MQTT_TOPIC_TYPE = "MQTT"; //$NON-NLS-1$
    public static final String ASSET_MQTT_SERVER_TYPE = "MQServer"; //$NON-NLS-1$
    
    public static final String PROPERTY_SRC_NAME = "srcName"; //$NON-NLS-1$
    public static final String PROPERTY_SRC_TYPE = "srcType"; //$NON-NLS-1$
    
    public static final String PROPERTY_SRC_PARENT_PREFIX = "srcParent"; //$NON-NLS-1$
    public static final String PROPERTY_PARENT_TYPE = "parentType"; //$NON-NLS-1$
    
    public static final String PROPERTY_PARENT_PREFIX = "p1"; //$NON-NLS-1$
    
    public static final String PROPERTY_INPUT_OPERATOR_TYPE = "inputOperatorType"; //$NON-NLS-1$
    public static final String PROPERTY_OUTPUT_OPERATOR_TYPE = "outputOperatorType"; //$NON-NLS-1$

}