---
title: "Using streamsx.kafka with Kerberos secured Kafka servers"
permalink: /docs/user/UsingKerberos/
excerpt: "How to configure the toolkit operators for use with Kerberos secured Kafka servers"
last_modified_at: 2020-11-06T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}
# Abstract

This document describes how to use the SPL operators of the streamsx.kafka toolkit to
connect with a Kerberos secured Kafka cluster.

Please note, that external links to the Confluent documentation link to the Confluent platform 5.5.2, which includes and
therefore refers to Kafka version 2.5.1.

## Preface

### Terms and Conventions

The term *GSSAPI* stands for Generic Security Service API, for which Kerberos is the dominant implementation. The official Kafka
documentation treats GSSAPI synonymuously with Kerberos.

A Kerberos *realm* is the domain over which a Kerberos authentication server has the authority to authenticate a user, host or service.
A realm name is often, but not always the upper case version of the name of the DNS domain over which it presides.

A Kerberos *principal* is a unique identity to which Kerberos can assign tickets. Principals can have an arbitrary number of components.
Each component is separated by a component separator, generally `/`. The last component is the realm, separated from the rest of the
principal by the realm separator, generally `@`. If there is no realm component in the principal, then it will be assumed that the
principal is in the default realm for the context in which it is being used.

Traditionally, a principal is divided into three parts: the primary, the instance, and the realm.
The format of a typical Kerberos principal is `primary/instance@REALM`.

### Kerberos requirements
#### Time synchronization

The Kerberos protocol requires the time of the client and server to match: if the system clocks of the
client does not match that of the server, authentication will fail. *Clients* in the context of Kerberos
are the Kafka clients, the Kafka brokers, and possibly the zookeeper nodes. The *servers* are the Key Distribution Centers (KDC),
or Kerberos servers. The simplest way to synchronize the system clocks is to use a Network Time Protocol (NTP) server.

**Note:** Active Directory Domain Controllers are typically also NTP servers.

#### Keytab files

Keytab files contain cryptographic key material for one or more principals, which is sensitive data. They must be protected as good as possible.
They must be readable by the Kerberos clients.

#### Hostname resolution

Each server in a Kerberos authentication realm must be assigned a Fully Qualified Domain Name (FQDN) that is forward-resolvable.
Kerberos also expects the server's FQDN to be reverse-resolvable. If reverse domain name resolution is not available,
set the *rdns* variable in the *\[libdefaults\]* section to `false` in the Kerberos clients' `krb5.conf` file.

## Configuring Kerberos for the Kafka servers

The necessary steps are described in the [Confluent documentation](https://docs.confluent.io/5.5.2/kafka/authentication_sasl/authentication_sasl_gssapi.html#kafka-sasl-auth-gssapi).
For setting up authentication between the brokers and the zookeeper and within the zookeeper ensemble, additional steps are required.
The steps to setup GSSAPI over TLS are also not part of this article. When TLS is required, the [Encryption with TLS section](https://docs.confluent.io/5.5.2/kafka/encryption.html)
of the Confluence documentation is a good source.

Briefly, the required steps are

- create principals for each Kafka broker.
- create a keytab file (one for each broker, or a common file that contains keys for all principals), safely transfer them to the brokers
- prepare the Kerberos configuration file `krb5.conf` - all brokers can have the same file
- create a JAAS configuration file - make sure to configure the login module that is included in the security provider of the JRE that runs the brokers.
  For IBM Java, use `com.ibm.security.auth.module.Krb5LoginModule`, for openJDK or Oracle Java use `com.sun.security.auth.module.Krb5LoginModule` and
  the corresponding parameters of the login module.
- configure GSSAPI in the brokers configuration files, typically `server.properties`.
- For SASL_SSL, also the TLS encryption settings must be configured.

## Operator setup

### Precondition

1. You have created principals in the KDC for the consumer and the producer.
2. You have created a keytab file for the principal and have transferred it in a secure manner to the Streams servers or to your SPL projects *etc* directory.

When you create a consumer group by using user defined parallelism (`@parallel` annotation in SPL) you most
likely share one principal and one keytab file with all replicated consumers.

### Kafka properties

IBM Streams uses the IBM JSSE2 security provider. This must be taken into account when setting up the JAAS configuration
as part of the consumer or producer configuration. The properties for the security configuration of consumers and producers
are the same. Their values can be different when different principals and keytab files are used. The term *Kafka properties*
will be used interchangeably with both configurations. A Kafka property can span
several lines in a property file. In this case, a line must be terminated with backslash.

#### Login module

    sasl.jaas.config=com.ibm.security.auth.module.Krb5LoginModule required \
        debug=true \
        credsType=both \
        useKeytab="<url_or_file_location>" \
        principal="<user@REALM>" ;

The keytab file must contain keys for the specified principal. It can be specified in URL style `useKeytab="file:////opt/consumer.keytab"`
or as file path `useKeytab="/opt/consumer.keytab"`. The keytab file can also be placed into the `etc` directory of the application and
can be included automatically into the Streams Application Bundle when the application is built. To do this, use the
`{applicationDir}` token, for example:

    useKeytab="{applicationDir}/etc/consumer.keytab"

The `principal` value is the Kerberos principal of the consumer or prooducer, for example `kafka-consumer-client@MY-DOMAIN.COM`.
Here, *MY-DOMAIN.COM* is the Kerberos REALM.


Debugging should be disabled (set to `false`) once the setup succeeded. Permanent debugging floods the stdout/stderr trace of the PE and has a negative impact to the performance.

**Don't forget to terminate the `sasl.jaas.config` value with a semicolon.**

#### Other security related Kafka properties required for Kerberos

    sasl.mechanism=GSSAPI
    security.protocol=SASL_PLAINTEXT (or SASL_SSL, when the connection with the brokers is encrypted with TLS)
    sasl.kerberos.service.name=<primary prinicipal name of the brokers>

With `security.protocol=SASL_SSL` it may be necessary to create a truststore when the server certificate cannot be trusted.
The trust store file must be configured in `ssl.truststore.location`, its password in `ssl.truststore.password`.

The `sasl.kerberos.service.name` property must be the primary principal name that the Kafka brokers run as.
Often it is simply "kafka". For example, when the service principals of the Kafka brokers are `kafka260/<hostname>@<REALM>`,
the `sasl.kerberos.service.name` property would be set to `kafka260`.

### Kerberos configuration

The essential Kerberos configuration information for the client is the default realm and the default KDC. The KDC is the Key Distribution Center (Kerberos Server). These two pieces of information can be specified as System properties

- `java.security.krb5.realm`
- `java.security.krb5.kdc`

If you set one of these properties you must set them both.

Often the configuration is done with configuration file *krb5.conf*, which contains the Kerberos configuration for the client.
Pay attention to the `include` and `includedir` directives as they would include files from the hosts or containers where
the application is scheduled to.

The Java security provider uses following search order for this file:

1. Java System property `java.security.krb5.conf` for example `java.security.krb5.conf=/user_directory/krb5.conf`

   Since [toolkit version 3.1.2](https://github.com/IBMStreams/streamsx.kafka/releases/tag/v3.1.2) the Kerberos
   configuration file can also be placed within the *etc* directory of the application to get included into the
   application bundle. This is useful in containerized environments like Cloud Pak for Data and in
   Streaming Analytics Service in the IBM cloud as far as the requirements for time synchronization and hostname
   resolution are met within the containers. Use the `{applicationDir}` placeholder like this:
   `java.security.krb5.conf={applicationDir}/etc/krb5.conf`.

2. *Java install*/lib/security/krb5.conf, which is `$STREAMS_INSTALL/java/jre/lib/security/krb5.conf` in a Streams runtime environment

   In a default Streams installation there is no such file.

3. `/etc/krb5.conf`

The system properties are specified as `vmArg` parameter to the operators. Examples:

    stream <MessageType.StringMessage> ReceivedMsgs = KafkaConsumer() {
      param
        groupId: "group1";
        topic: "topic";
        propertiesFile: "etc/consumer.properties";
        vmArg: "-Djava.security.krb5.realm=EXAMPLE.DOMAIN.COM", // per convention, realms are upper case
          "-Djava.security.krb5.kdc=kdc-host.domain.com";  // per convention, hostnames are lowercase
    }

or

    stream <MessageType.StringMessage> ReceivedMsgs = KafkaConsumer() {
      param
        groupId: "group1";
        topic: "topic";
        propertiesFile: "etc/consumer.properties";
        vmArg: "-Djava.security.krb5.conf={applicationDir}/etc/myKrb5.conf";
    }

# Useful links

Keep in mind, that nearly all resources in the Internet refer to Oracle Java or openJDK/JRE.

[Confluent Kerberos client security](https://docs.confluent.io/5.5.2/kafka/authentication_sasl/authentication_sasl_gssapi.html#clients)


[IBM Kerberos login module](https://www.ibm.com/support/knowledgecenter/SSYKE2_7.1.0/com.ibm.java.security.api.71.doc/jgss/com/ibm/security/auth/module/Krb5LoginModule.html)
