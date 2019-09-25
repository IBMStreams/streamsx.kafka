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

import org.apache.log4j.Level;

/**
 * Convenience methods for access to system properties for tweeking Producer and Consumer.
 * Following properties can be used:
 * <ul>
 * <li>{@value #KAFKA_OP_TRACE_DEBUG} - override the DEBUG level for trace messages; example: "-Dkafka.op.trace.debug=info"</li>
 * <li>{@value #KAFKA_METRICS_TRACE_DEBUG} - override the DEBUG level for periodic Kafka metric output; example: "-Dkafka.metrics.trace.debug=info"</li>
 * <li>{@link #KAFKA_OP_LEGACY} - 1 or 'true' enables v1.x legacy behavior when not in consistent region; example: "-Dkafka.op.legacy=true"</li>
 * <li>{@value #KAFKA_PREFETCH_MIN_FREE_MB} - override (increase) the default minimum free memory for low memory detection; example: "-Dkafka.prefetch.min.free.mb=200"</li>
 * <li>{@value #KAFKA_MEM_CHECK_THRESHOLD}</li>
 * </ul> 
 * @author IBM Kafka toolkit maintainers
 */
public class SystemProperties {

    private static final String KAFKA_PREFETCH_MIN_FREE_MB = "kafka.prefetch.min.free.mb";
    private static final String KAFKA_MEM_CHECK_THRESHOLD = "kafka.memcheck.threshold.factor";
    private static final String KAFKA_OP_TRACE_DEBUG = "kafka.op.trace.debug";
    private static final String KAFKA_METRICS_TRACE_DEBUG = "kafka.metrics.trace.debug";
    private static final String KAFKA_OP_LEGACY = "kafka.op.legacy";

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
        final String prop = System.getProperty (KAFKA_OP_TRACE_DEBUG);
        if (prop == null) return Level.DEBUG;
        try {
            // unparsable values are converted to DEBUG; exception is never thrown
            return Level.toLevel (prop.toUpperCase());
        } catch (Exception e) {
            System.err.println (KAFKA_OP_TRACE_DEBUG + ": " + e);
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
        final String prop = System.getProperty (KAFKA_METRICS_TRACE_DEBUG);
        if (prop == null) return Level.TRACE;
        try {
            // unparsable values are converted to DEBUG; exception is never thrown
            return Level.toLevel (prop.toUpperCase());
        } catch (Exception e) {
            System.err.println (KAFKA_METRICS_TRACE_DEBUG + ": " + e);
            return Level.TRACE;
        }
    }

    /**
     * The system property 'kafka.op.legacy' can be used to disable the behavioral changes that require a JCP.
     * The property must have the value "true" (case insensitive) or "1". 
     * @return true, when the property kafka.op.legacy is '1' or 'true', false otherwise.
     */
    public static boolean isLegacyBehavior() {
        final String prop = System.getProperty (KAFKA_OP_LEGACY);
        if (prop == null) return false;
        if (prop.equalsIgnoreCase ("true")) return true;
        if (prop.equals ("1")) return true;
        return false;
    }

    /**
     * The factor by which 'max.poll.records' is multiplied as a threshold for memory check before fetch
     * @param defaultValue the default value returned when the property {@value #KAFKA_MEM_CHECK_THRESHOLD} is not set or the property cannot be parsed to a positive integer.
     * @return the number, by which 'max.poll.records' is multiplied as threshold
     */
    public static int getMemoryCheckThresholdMultiplier (int defaultValue) {
        final String prop = System.getProperty (KAFKA_MEM_CHECK_THRESHOLD);
        if (prop == null) return defaultValue;
        try {
            final int f = Integer.parseInt (prop);
            if (f >= 0) return f;
            System.err.println (KAFKA_MEM_CHECK_THRESHOLD + " must be non-negative. Using " + defaultValue + " instead of " + f);
            return defaultValue;
        }
        catch (Exception e) {
            System.err.println (KAFKA_MEM_CHECK_THRESHOLD + ": " + e);
            return defaultValue;
        }
    }


    /**
     * The system property 'kafka.prefetch.min.free.mb' can be used to override the minimum free memory before messages are fetched.
     * @return the property value multiplied by 2^20 (1M) or 0 if the property is not used.
     */
    public static long getPreFetchMinFreeMemory() {
        final String prop = System.getProperty (KAFKA_PREFETCH_MIN_FREE_MB);
        if (prop == null) return 0L;
        try {
            final long mb = Long.parseLong (prop);
            return mb * 1024L * 1024L;
        }
        catch (Exception e) {
            System.err.println (KAFKA_PREFETCH_MIN_FREE_MB + ": " + e);
            return 0L;
        }
    }
}
