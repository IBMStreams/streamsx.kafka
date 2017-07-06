package com.ibm.streamsx.kafka.properties;

import org.apache.log4j.Logger;

public class JaasUtil {

    public static final String SASL_JAAS_PROPERTY = "sasl.jaas.config"; //$NON-NLS-1$
    private static final String JAAS_TEMPLATE = "org.apache.kafka.common.security.plain.PlainLoginModule required" + //$NON-NLS-1$
            " serviceName=\"kafka\"" + //$NON-NLS-1$
            " username=\"%s\"" + //$NON-NLS-1$
            " password=\"%s\";"; //$NON-NLS-1$

    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(JaasUtil.class);

    public static String getSaslJaasPropertyValue(String username, String password) {
        return String.format(JAAS_TEMPLATE, username, password);
    }
}
