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
package com.ibm.streamsx.kafka.i18n;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import com.ibm.streamsx.kafka.MsgFormatter;

public class Messages {
    private static final String BUNDLE_NAME = "com.ibm.streamsx.kafka.i18n.KafkaMessages"; //$NON-NLS-1$

    private static /*final*/ ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle (BUNDLE_NAME);
    private static final ResourceBundle FALLBACK_RESOURCE_BUNDLE = ResourceBundle.getBundle (BUNDLE_NAME, new Locale ("en", "US"));

    private Messages() {
    }

    public static String getString(String key) {
        try {
            return getRawMsg (key);
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    public static String getString (String key, Object... args) {
        try {
            String msg = getRawMsg (key);
            if (args == null) return msg;
            return MsgFormatter.format (msg, args);
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    /**
     * prints out all messages with generic parameters PARAM_0 ... PARAM_9
     * @param args
     */
    public static void main (String[] args) {
        List<Locale> locales = new ArrayList<>(12);
        locales.add(new Locale("de", "DE"));
        locales.add(new Locale("en", "US"));
        locales.add(new Locale("fr", "FR"));
        locales.add(new Locale("it", "IT"));
        locales.add(new Locale("ja", "JP"));
        locales.add(new Locale("ko", "KR"));
        locales.add(new Locale("pt", "BR"));
        locales.add(new Locale("ru", "RU"));
        locales.add(new Locale("zh", "CN"));
        locales.add(new Locale("zh", "TW"));
        // locales, for which we do not have a resource file: Fallback to what?
        locales.add(new Locale("fr", "CA"));
        locales.add(new Locale("hu", "HU"));

        Enumeration<String> keyEnum = RESOURCE_BUNDLE.getKeys();
        Map<String, String> messageMap = new HashMap<>();
        int maxKeyLen = 0;
        Object[] parameters = new Object[] {"PARAM_0", "PARAM_1", "PARAM_2", "PARAM_3", "PARAM_4", "PARAM_5", "PARAM_6", "PARAM_7", "PARAM_8", "PARAM_9"}; 
        while (keyEnum.hasMoreElements()) {
            String key = keyEnum.nextElement();
            if (key.length() > maxKeyLen) maxKeyLen = key.length();
            String message = getString (key, parameters);
            messageMap.put (key, message);
        }
        // sort according message (CDIST numbers)
        List<Entry<String, String>> keysMessages = new LinkedList<>(messageMap.entrySet());
        Collections.sort (keysMessages, new Comparator<Entry<String, String>>() {
            @Override
            public int compare (Entry<String, String> o1, Entry<String, String> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        int n = 0;
        for (Entry <String, String> msg: keysMessages) {
            String k = msg.getKey();
            while (k.length() < maxKeyLen) k += ' ';
            String ns = "" + ++n;
            while (ns.length() < 3) ns += ' ';
            for (Locale loc: locales) {
                RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME, loc);
                System.out.println (MsgFormatter.format ("{0} {1}  {2}  {3}", ns, loc, k, getString(msg.getKey(), parameters)));
            }
            System.out.println();
        }
    }


    private static String getRawMsg (String key) throws MissingResourceException {
        try {
            return RESOURCE_BUNDLE.getString(key);
        } catch (MissingResourceException e) {
            return FALLBACK_RESOURCE_BUNDLE.getString(key);
        }
    }
}
