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

/**
 * @author IBM Kafka toolkit maintainers
 */
public class Features {
    /**
     * When set to true, consumer groups outside a consistent region with startPosition != Default are enabled.
     * When set to false, group management is automatically disabled when startPosition != Default and not in a CR. 
     * This feature requires a JobControlPlane.
     */
    public static boolean ENABLE_NOCR_CONSUMER_GRP_WITH_STARTPOSITION = !SystemProperties.isLegacyBehavior();

    /**
     * When set to true, the consumer does not seek to initial startPosition when not in consistent region.
     * When false, the consumer seeks to what startPosition is after every restart.
     * This feature requires a JobControlPlane.
     */
    public static boolean ENABLE_NOCR_NO_CONSUMER_SEEK_AFTER_RESTART = !SystemProperties.isLegacyBehavior();
}
