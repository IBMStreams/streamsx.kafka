---
title: "Using streamsx.kafka with Kerberos secured Kafka servers"
permalink: /docs/user/UsingKerberos/
excerpt: "How to configure the toolkit operators for use with Kerberos secured Kafka servers"
last_modified_at: 2020-08-24T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}
# Abstract

**This document is stil work in progress.**

This document describes how to use the SPL operators of the streamsx.kafka toolkit to
connect with Kerberos secured Kafka servers.

## Operator setup

IBM Streams uses the IBM JSSE2 security provider. This must be taken into account when setting up the JAAS configuration as part ofthe consumer or producer configuration.
The security configuration is equivalent for consusmers and producers, so that the term *Kafka properties* will be used interchangeably with both configurations. A Kafka property can span several lines in a property file. In this case, a line must be terminated with backslash.

### Kafka properties

#### Login module

    sasl.jaas.config=com.ibm.security.auth.module.Krb5LoginModule required \
        debug=true \
        credsType=both \
        useKeytab="<url_or_file_location>" \
        principal="<user/host@REALM>" ;

The keytab file can be specified in URL style `useKeytab="file:////opt/krb5.keytab"` or as file path `useKeytab="/opt/krb5.keytab"`.
The keytab file can also be placed into the `etc` directory of the application and can be included automatically into the Streams Application Bundle. To do this, use the `{applicationDir}` token, for example:

    useKeytab="{applicationDir}/etc/myKeytab.keytab"

The `principal` value is the Kerberos principal, for example `user/host@REALM`. Here, host is the host of the center for key distribution and REALM is the Kerberos REALM.


Debugging should be disabled (set to `false`) once the setup succeeded. Permanent debugging floods the stdout/stderr trace of the PE and has a negative impact to the performance.

**Don't forget to add a terminating semicolon to the `sasl.jaas.config` value.**

#### Other security related Kafka properties required for Kerberos

    sasl.mechanism=GSSAPI
    security.protocol=SASL_PLAINTEXT (or SASL_SSL)
    sasl.kerberos.service.name=kafka

With `security.protocol=SASL_SSL` it may be necessary to create a truststore when the server certificate cannot be trusted. The trust store file must be configured in `ssl.truststore.location`, its password in `ssl.truststore.password`.

The `sasl.kerberos.service.name` property is the name of the Kerberos service used by Kafka. This name must match the principal name of the Kafka brokers. Often it is simply "kafka".

### Kerberos configuration

The essential Kerberos configuration information for the client is the default realm and the default KDC. The KDC is the Key Distribution Center (Kerberos Server). These two pieces of information can be specified as System properties

- `java.security.krb5.realm`
- `java.security.krb5.kdc`

If you set one of these properties you must set them both.

Often the configuration this is done with configuration file **krb5.conf**, which contains the Kerberos configuration for the client. The security provider uses following search order for this file:

1. Java System property `java.security.krb5.conf` for example `java.security.krb5.conf=/user_directory/krb5.conf`

   **Cannot be used with krb5.conf in `<application dir>/etc` as the application dir is not known when configuring the property.**

2. *Java install*/lib/security/krb5.conf, which is `$STREAMS_INSTALL/java/jre/lib/security/krb5.conf` in a Streams runtime environment

3. `/etc/krb5.conf`

The system properties are specified as `vmArg` parameter to the operators. Example:

    stream <MessageType.StringMessage> ReceivedMsgs = KafkaConsumer() {
      param
        groupId: "group1";
        topic: "topic";
        propertiesFile: "etc/consumer.properties";
        vmArg: "-Djava.security.krb5.realm=EXAMPLE.DOMAIN.COM", // per convention, realms are upper case
          "-Djava.security.krb5.kdc=kdc_host.domain.com";  // per convention, hostnames are lowercase
    }

# Useful links

Keep in mind, that nearly all resources in the Internet refer to Oracle Java or openJDK/JRE.

[Confluent Kerberos client security](https://docs.confluent.io/4.0.0/kafka/authentication_sasl_gssapi.html#clients)


[IBM Kerberos login module](https://www.ibm.com/support/knowledgecenter/SSYKE2_7.1.0/com.ibm.java.security.api.71.doc/jgss/com/ibm/security/auth/module/Krb5LoginModule.html)
