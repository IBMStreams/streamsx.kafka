package com.ibm.streamsx.kafka.clients.consumer;

/**
 * This interface defines pseudo offsets, which can be used to control
 * seeking the beginning or end of topic partitions.
 */
public interface OffsetConstants {
    public static final long SEEK_END = -1l;
    public static final long SEEK_BEGINNING = -2l;
    public static final long NO_SEEK = -3l;
    public static final long HIGHEST_VALID_OFFSET_VALUE = NO_SEEK;
}
