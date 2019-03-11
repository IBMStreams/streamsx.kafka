package com.ibm.streamsx.kafka;

import org.apache.log4j.Level;

public class SystemProperties {

    /**
     * The system property kafka.op.trace.debug can be used to override the severity for debug messages.
     * Use the enum values of {@link org.apache.log4j.Level} enum as values for the property.
     * Example: Using
     *  
     * '-Dkafka.op.trace.debug=info'
     * 
     * traces selected debug messages with INFO severity.
     * 
     * <b>Note:</b> this feature is not yet fully implemented in all client implementations.
     * 
     * @return The level from the property value or Level.DEBUG
     */
    public static Level getDebugLevelOverride() {
        String prop = System.getProperty ("kafka.op.trace.debug");
        if (prop == null) return Level.DEBUG;
        try {
            return Level.toLevel (prop.toUpperCase());
        } catch (Exception e) {
            e.printStackTrace();
            return Level.DEBUG;
        }
    }


    /**
     * The system property kafka.trace.debug can be used to override the severity for metrics debug messages.
     * Use the enum values of {@link org.apache.log4j.Level} enum as values for the property.
     * Example: Using
     *  
     * '-Dkafka.metrics.trace.debug=info'
     * 
     * traces operator metrics with INFO severity.
     * 
     * <b>Note:</b> this feature is not yet fully implemented in all client implementations.
     * 
     * @return The level from the property value or Level.TRACE
     */
    public static Level getDebugLevelMetricsOverride() {
        String prop = System.getProperty ("kafka.metrics.trace.debug");
        if (prop == null) return Level.TRACE;
        try {
            return Level.toLevel (prop.toUpperCase());
        } catch (Exception e) {
            e.printStackTrace();
            return Level.TRACE;
        }
    }

    /**
     * The system property 'kafka.op.legacy' can be used to disable the behavioral changes that require a JCP.
     * The property must have the value "true" (case insensitive) or "1". 
     * @return true, when the property kafka.op.legacy is '1' or 'true', false otherwise.
     */
    public static boolean isLegacyBehavior() {
        String prop = System.getProperty ("kafka.op.legacy");
        if (prop == null) return false;
        if (prop.equalsIgnoreCase ("true")) return true;
        if (prop.equals ("1")) return true;
        return false;
    }
}
