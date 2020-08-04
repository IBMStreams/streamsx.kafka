---
title: "Debugging SSL issues"
permalink: /docs/user/debugging_ssl_issues/
excerpt: "How to trouble shoot SSL issues"
last_modified_at: 2019-04-04T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

## Introduction

The Kafka operators can be configured to use [SSL for encryption and authentication](https://kafka.apache.org/documentation/#security_ssl).
Issues in this area can be trouble-shooted by enabling SSL debugging.

## Toolkit version 2.0 and older

Set the `javax.net.debug` system property using the **vmArg** parameter.

**Example:**

    stream <MessageType.StringMessage, MessageType.ConsumerMessageMetadata> ReceivedMessages as O = KafkaConsumer() {
        param
            propertiesFile: "etc/consumer.properties";    // here we set up the SSL related configs
            topic: "topic1", "topic2";
            clientId: "consumerClient-0";
            groupId: "group-0";

            vmArg: "-Djavax.net.debug=ssl:trustmanager";
    }

**Hint:** If you need to specify multiple arguments for Java, specify multiple values for the **vmArg** parameter:

    vmArg: "-Djavax.net.debug=ssl:trustmanager", "-Xmx1G";

## Toolkit version 2.1 and newer

Use the optional **sslDebug** operators parameter to turn on SSL debugging. When both, `-Djavax.net.debug` via **vmArg**, and **sslDebug** operator
parameter is used, the **sslDebug** parameter is ignored regardless of its value.

**Example:**

    stream <MessageType.StringMessage, MessageType.ConsumerMessageMetadata> ReceivedMessages as O = KafkaConsumer() {
        param
            propertiesFile: "etc/consumer.properties";    // here we set up the SSL related configs
            topic: "topic1", "topic2";
            clientId: "consumerClient-0";
            groupId: "group-0";
            sslDebug: true;   // equivalent to javax.net.debug=all
    }

For finer debugging specifications, you can of course also use the way described for the older toolkit versions.

## Fine-grained debugging options

The options that can be used for the `javax.net.debug=<x>` system property are described in the
[IBM Knowledge Center](https://www.ibm.com/support/knowledgecenter/en/SSYKE2_7.1.0/com.ibm.java.security.component.71.doc/security-component/jsse2Docs/debug.html).

Where _&lt;x&gt;_ is one of :

| x | description |
| --- | --- |
| `help` | prints out this help |
| `all`  | turn on all debugging |
| `true` | turn on all debugging, for compatibility |
| `ssl`  | turn on ssl debugging |

The following can be used with **ssl**:

| ssl:x | description |
| --- | --- |
| `record`       | enable per-record tracing |
| `handshake`    | print each handshake message |
| `keygen`       | print key generation data |
| `session`      | print session activity |
| `defaultctx`   | print default SSL initialization |
| `sslctx`       | print SSLContext tracing |
| `sessioncache` | print session cache tracing |
| `keymanager`   | print key manager tracing |
| `trustmanager` | print trust manager tracing |
| `nio`          | print nio tracing |
| `pluggability` | print pluggability tracing |

**handshake** debugging can be widened with:

| ssl`:handshake:`x | description |
| --- | --- |
| `data`    | hex dump of each handshake message |
| `verbose` | verbose handshake message printing |

**record** debugging can be widened with:

| ssl:record:x | description |
| --- | --- |
| `plaintext` | hex dump of record plaintext |
| `packet`    | print raw SSL/TLS packets |


**Examples:**

    vmArg: "-Djavax.net.debug=ssl:trustmanager,session,handshake:verbose";
    vmArg: "-Djavax.net.debug=ssl:nio,session";

Unfortunately the SSL trace appears in stdout without timestamps, so that they cannot be correlated with PE operator trace.
Turning on debugging for SSL has no effect (also no negative effect) as long as no SSL provider is active within the Java virtual machine.