/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

/**
 * The mode how the consumer is subscribed. 
 * A consumer can be assigned to one or more topic partitions, or a consumer can be subscribed to one or more topics, but never a combination of both.
 */
public enum SubscriptionMode {
    NONE, SUBSCRIBED, ASSIGNED;
}
