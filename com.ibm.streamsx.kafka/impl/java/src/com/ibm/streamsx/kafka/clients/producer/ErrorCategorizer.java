package com.ibm.streamsx.kafka.clients.producer;

public interface ErrorCategorizer {
    public static enum ErrCategory {
        ABORT_PE,
        RECOVERABLE,
        FINAL_FAIL
    }

    public ErrCategory isRecoverable (Exception e);
}
