---
title: "Using streamsx.kafka with IBM Event Streams on-premises"
permalink: /docs/user/UsingEventStreamsOnPrem/
excerpt: "How to configure the toolkit operators for use with Eventstreams on-premise"
last_modified_at: 2021-04-13T11:50:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}
# Abstract

This document describes how to use the SPL operators of the streamsx.kafka toolkit to
connect with Eventstreams installed on-premises.

**For a connection with Event Streams as IBM Cloud service, this article does not apply.**
Please use the [streamsx.messagehub](https://github.com/IBMStreams/streamsx.messagehub) toolkit for this purpose.

# What is IBM Event Streams

IBM Event Streams is a Kafka distribution for [IBM cloud private](https://www.ibm.com/docs/en/cloud-private/3.2.0?topic=paks-event-streams).
Please note, that it has been deprecated in IBM Cloud Pak for Integration. IBM recommends to use the Confluent Platform instead.

# How to connect

In the on-prem Event Streams admin console, there is an option to create all the TLS related security credentials.
It then automatically generates the Kafka client properties file contents.

These auto-generated Kafka client properties content can be used to configure the KafkaConsumer and KafkaProducer operator. Of course,
you can add more consumer or producer related configs, like a Kafka group identifier (`group.id`) for consumers.

The properties can be specified

- in a text file (use the **propertiesFile** parameter to denote the filename), or
- in an application configuration (use the **appConfigName** parameter to denote the name of the application configuration)

# Useful links

[Event Streams documentation](https://ibm.github.io/event-streams/)