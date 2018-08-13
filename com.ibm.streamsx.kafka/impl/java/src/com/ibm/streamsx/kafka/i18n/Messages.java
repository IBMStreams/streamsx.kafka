package com.ibm.streamsx.kafka.i18n;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Messages {
    private static final String BUNDLE_NAME = "com.ibm.streamsx.kafka.i18n.KafkaMessages"; //$NON-NLS-1$

    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle (BUNDLE_NAME);
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

    public static String getString(String key, Object... args) {
        try {
            String msg = getRawMsg (key);
            return MessageFormat.format(msg, args);
        } catch (MissingResourceException e) {
            return '!' + key + '!';
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
