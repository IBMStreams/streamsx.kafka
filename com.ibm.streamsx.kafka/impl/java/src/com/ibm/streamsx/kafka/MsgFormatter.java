/**
 * 
 */
package com.ibm.streamsx.kafka;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains a static wrapper for MessageFormat.format, which avoids
 * throwing exceptions when the objects cannot be formatted for the given format string.
 * 
 * Exceptions are printed to stderr with stack trace.
 * 
 * @author the IBM Kafka toolkit maintainers
 * @since toolkit version 2.2
 */
public class MsgFormatter {
    private final static Map<String, MessageFormat> formatCache = Collections.synchronizedMap(new HashMap<>());
    
    /**
     * Formats a pattern with parameters.
     * @param pattern   the pattern
     * @param arguments the parameters
     * @return
     */
    public static String format (String pattern, Object... arguments) {
        MessageFormat fmt = formatCache.get (pattern);
        if (fmt == null) {
            fmt = new MessageFormat (pattern);
            formatCache.put (pattern, fmt);
        }
        try {
            return fmt.format (arguments, new StringBuffer(), null).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to format trace message. Please write an issue on GitHub streamsx.kafka; see stdouterr";
        }
    }
}
