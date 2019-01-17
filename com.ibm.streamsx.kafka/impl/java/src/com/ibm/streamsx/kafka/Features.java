/**
 * 
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
    public static boolean ENABLE_NOCR_CONSUMER_GRP_WITH_STARTPOSITION = false;
    /**
     * When set to true, the consumer does not seek to initial startPosition when not in consistent region.
     * When false, the consumer seeks to what startPosition is after every restart.
     * This feature requires a JobControlPlane.
     */
    public static boolean ENABLE_NOCR_NO_CONSUMER_SEEK_AFTER_RESTART = false;
}
