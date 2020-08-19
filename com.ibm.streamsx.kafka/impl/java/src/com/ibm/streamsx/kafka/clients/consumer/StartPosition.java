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
package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum StartPosition {
    Beginning ("beginning", "begin", "first", "start"),
    End ("end", "last", "latest", "oldest"),
    Default ("default"),
    Time ("time", "timestamp"),
    Offset ("offset", "offs");

    private Set<String> matches = new HashSet<>();

    /**
     * @param matchesLower
     */
    private StartPosition (String... matchesLower) {
        Collections.addAll (this.matches, matchesLower);
    }

    /**
     * Returns the enum from a String match
     * @param s The String
     * @return the StartPosition that matches the String
     * @throws IllegalArgumentException invalid String value
     */
    public static StartPosition ofString (final String s) {
        final String sL = s.toLowerCase();
        for (StartPosition sp: StartPosition.values()) {
            if (sp.matches.contains(sL)) return sp;
        }
        throw new IllegalArgumentException("Illegal String value for StartPosition: " + s);
    }
}
