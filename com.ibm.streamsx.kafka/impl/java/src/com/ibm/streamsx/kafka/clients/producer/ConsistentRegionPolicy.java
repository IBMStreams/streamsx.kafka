package com.ibm.streamsx.kafka.clients.producer;


public enum ConsistentRegionPolicy {
    AtLeastOnce,
    NonTransactional,
    Transactional;
}