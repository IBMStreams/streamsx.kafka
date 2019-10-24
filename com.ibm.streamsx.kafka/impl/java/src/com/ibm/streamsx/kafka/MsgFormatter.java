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
