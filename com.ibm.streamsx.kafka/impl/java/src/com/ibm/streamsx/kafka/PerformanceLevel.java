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

public class PerformanceLevel extends Level {
    private static final long serialVersionUID = 1L;

    public static final int PERF_INT = 100000;
    public static final String PERF_NAME = "PERF"; //$NON-NLS-1$
    
    public static final Level PERF = new PerformanceLevel(PERF_INT, PERF_NAME, 10);
    
    protected PerformanceLevel(int level, String levelStr, int syslogEquivalent) {
        super(level, levelStr, syslogEquivalent);
    }

    public static Level toLevel(String logArg) {
        if(logArg != null & logArg.toUpperCase().equals(PERF_NAME))
            return PERF;
        
        return Level.toLevel(logArg);
    }

    public static Level toLevel(int val) {
        if(val == PERF_INT)
            return PERF;
        
        return Level.toLevel(val);
    }
    
    public static Level toLevel(int val, Level defaultLevel) {
        if(val == PERF_INT)
            return PERF;
        
        return Level.toLevel(val, defaultLevel);
    }
    
    public static Level toLevel(String logArg, Level defaultLevel) {
        if(logArg != null && logArg.toUpperCase().equals(PERF_NAME))
            return PERF;
        
        return Level.toLevel(logArg, defaultLevel);
    }
}
