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
package com.ibm.streamsx.kafka.clients.consumer;

/**
 * The mode how the consumer is subscribed. 
 * A consumer can be assigned to one or more topic partitions, or a consumer can be subscribed to one or more topics, but never a combination of both.
 */
public enum SubscriptionMode {
    NONE, SUBSCRIBED, ASSIGNED;
}
