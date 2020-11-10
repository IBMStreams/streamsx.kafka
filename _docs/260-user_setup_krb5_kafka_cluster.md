---
title: "Setting up a kerberized Kafka test cluster"
permalink: /docs/user/SetupKrb5KafkaCluster/
excerpt: "How to setup and configure a Kafka cluster with Kerberos authentication"
last_modified_at: 2020-11-10T14:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}
# Abstract

This document describes how to setup a Kafka test cluster that is secured with Kerberos.
We are going to setup the cluster on RHEL 7 or Centos 7 virtual machines. This document contains

- steps to setup the basic KDC
- steps to create the principals and keytabs
- steps to configure the brokers

This document does *not* describe

- how to install Centos or RHEL
- how to configure NTP on RHEL or Centos
- how to install Kafka
- how to create a Certification Authority and how to create SSL keys and certificates for the brokers
- how to create ACLs to fine-grain access to Kafka resources

This description, especially configuring JAAS and the brokers, is mainly based on the
[Confluent documentation](https://docs.confluent.io/5.5.2/kafka/authentication_sasl/authentication_sasl_gssapi.html#).
Please note, that links to the Confluent documentation refer to the Confluent platform 5.5.2, which includes
Kafka version 2.5.1, and therefore refers to this Kafka version. The steps described here, have been verified
with Kafka 2.1.1.

## Preparation

### Virtual machines

In this description, we setup four virtual machines:

- three Kafka brokers
- one zookeeper node for the Kafka brokers

The zookeper node will act as the KDC for Kerberos (Kerberos server). Note, that in real-world scenarios,
a Kerberos server should always run on a dedicated host with no other services enabled.

The virtual machines require at least Java 8 to run Kafka. The 1.8 openjdk that ships with the Linux
distribution, is a good choice. openjdk 1.8 can also be used to run the zookeeper.

Kafka, and the zookeeper can be installed under a normal user account. Don't download and install
a separate zookeeper on the VM that acts as zookeeper. Simply install the same Kafka distribution on all
machines in the same manner and use the zookeeper that is distributed with Kafka.

To easily copy around files with *scp*, you should also create SSH keypairs on every system and add the
public key to all *authorized_keys* files on all VMs. Do this for the normal user and for root, so that you
can copy around files as root and as normal user.

### Meeting the requirements for Kerberos

Kerberos requires

- Time synchronization between all VMs and other potential Kerberos client machines
- Forward hostname resolution (revers resolution is optional)

When using virtual machines, it may be sufficient to enable time synchronization with the host
system (VMware has this option) or, if the virtualization does not offer such an option,
to configure NTP.

For hostname resolution, make sure that all servers participating in a realm,
(KDC, possibly zookeepers, Kafka servers, Kafka clients) resolve all hostnames in the same address,
and report their own FQDN in the `hostname` command. On RHEL 7, use the *hostnamectl set-hostname * command
to change the system's hostname.

When using */etc/hosts* for forward hostname resolution, make sure that all VMs use the same version
of that file and that the own hostname is never resolved to the loopback address *127.0.0.1*:

    127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
    ::1         localhost localhost.localdomain localhost6 localhost6.localdomain6

    192.168.245.100   kafka-0.localdomain   kafka-0
    192.168.245.101   kafka-1.localdomain   kafka-1
    192.168.245.102   kafka-2.localdomain   kafka-2
    192.168.245.110   zk-0.localdomain      zk-0

This description, uses *localdomain* as the domain. According to the Kerberos conventions, we are going to use the realm
LOCALDOMAIN later on, which is the uppercase version of the domain. You can, of course, use another domain and realm
for your setup, but make sure to replace things when you copy&paste commands.

On all VMs install following rpms (as root):

    # yum install krb5-libs krb5-workstation

On the zookeeper node, install in addition

    # yum install krb5-server

to enable it being the KDC. FYI: The *krb5\** packages contain the MIT implementation of Kerberos.
It is not the heimdal implementation.

## Setup the KDC

On the zookeeper node (which is also the KDC), configure the following items:

### /var/kerberos/krb5kdc/kadm5.acl

This is the ACL file for the kadmind daemon to manage access rights to the Kerberos database.
For our setup, this file should contain the following line. When you have a different realm, replace *LOCALDOMAIN*
by your realm.

    */admin@LOCALDOMAIN      *

For further reading, use `man kadm5.acl`.

### /var/kerberos/krb5kdc/kdc.conf

This is the configuration file for the KDC. The content is similar to the default
content after installation of the krb5-server package. Replace the realm *EXAMPLE.COM* by your realm:

    [kdcdefaults]
     kdc_ports = 88
     kdc_tcp_ports = 88

    [realms]
     LOCALDOMAIN = {
      #master_key_type = aes256-cts
      acl_file = /var/kerberos/krb5kdc/kadm5.acl
      dict_file = /usr/share/dict/words
      admin_keytab = /var/kerberos/krb5kdc/kadm5.keytab
      supported_enctypes = aes256-cts:normal aes128-cts:normal des3-hmac-sha1:normal arcfour-hmac:normal camellia256-cts:normal camellia128-cts:normal des-hmac-sha1:normal des-cbc-md5:normal des-cbc-crc:normal
     }

### /etc/krb5.conf

This file contains the Kerberos client configuration. The server that we are using as the KDC will also be a Kerberos client,
allowing users to authenticate via Kerberos to services that is hosts.
From the default file, uncomment all lines starting with comment (#) and replace the realm *EXAMPLE.COM* with your realm.
Replace also the domain in lowercase with your domain in the *\[domain_realm\]* section.

Make sure you set *rdns* to `false` if you have no revers DNS zone within your domain, or more generally, if
revers hostname resolution does not work.

In the section for your realm (in the example below *LOCALDOMAIN*) add your KDC, i.e. the FQDN of
the zookeeper node, as *kdc* and *admin_server*:

    # Configuration snippets may be placed in this directory as well
    includedir /etc/krb5.conf.d/

    [logging]
     default = FILE:/var/log/krb5libs.log
     kdc = FILE:/var/log/krb5kdc.log
     admin_server = FILE:/var/log/kadmind.log

    [libdefaults]
     dns_lookup_realm = false
     ticket_lifetime = 24h
     renew_lifetime = 7d
     forwardable = true
     rdns = false
     pkinit_anchors = FILE:/etc/pki/tls/certs/ca-bundle.crt
     default_realm = LOCALDOMAIN
     default_ccache_name = KEYRING:persistent:%{uid}

    [realms]
     LOCALDOMAIN = {
      kdc = zk-0.localdomain
      admin_server = zk-0.localdomain
     }

    [domain_realm]
     .localdomain = LOCALDOMAIN
     localdomain = LOCALDOMAIN

This file will later be distributed to the Kafka nodes and can also be used by other Kerberos clients,
for example Kafka prodcuers that need to authenticate. If you want to learn more about this
configuration file, use `man krb5.conf`.

### Create the Kerberos database

Run following command to create the database for your realm:

    # kdb5_util create -s -r LOCALDOMAIN

    Loading random data
    Initializing database '/var/kerberos/krb5kdc/principal' for realm 'LOCALDOMAIN',
    master key name 'K/M@LOCALDOMAIN'
    You will be prompted for the database Master Password.
    It is important that you NOT FORGET this password.
    Enter KDC database master key:
    Re-enter KDC database master key to verify:

Replace *LOCALDOMAIN* by your realm if it is different.

### Start and enable Kerberos services as system services

To permanently start and enable Kerberos for your KDC, run these commands:

    # systemctl start krb5kdc kadmin
    # systemctl enable krb5kdc kadmin

To verify the services are running, enter

    # systemctl status krb5kdc kadmin

To assign *root*, and a normal user admin rights, create an admin principals for root and the user.
The user should be the user, that you run Kafka as. Replace *rolef* by your username:

    # kadmin.local
    kadmin.local: addprinc root/admin
    kadmin.local: addprinc rolef/admin
    kadmin.local: quit

Enter passwords for the root and the user prinicipal. These principals allow you to use the *kadmin* tool
as root or a normal user from any host within your adminstrative domain, for example from a Kafka broker instead
of using the local version *kadmin.local* being logged in as *root* to the KDC.

## Configure Kafka servers to be kerberized

### Create principals and keytab files

For every Kafka broker, we create service principals and keys. The keys go into keytab files. We do everything directly as root on the KDC,
and create one file for the keys of all Kafka brokers. We could also generate separate keytab files for each Kafka broker,
directly on the Kafka node as the user running Kafka, but for this excercise it is sufficient to have one common file, that
we later distribute to all Kafka brokers. The keytab files are not required on the KDC, more precisely, they should not stay
there as they contain sensible data for the principals comparable with passwords belonging to user logins.

For every Kafka broker, run following command on the KDC:

    # kadmin.local -q 'addprinc -randkey kafka/kafka-0.localdomain@LOCALDOMAIN'
    # kadmin.local -q 'addprinc -randkey kafka/kafka-1.localdomain@LOCALDOMAIN'
    # kadmin.local -q 'addprinc -randkey kafka/kafka-2.localdomain@LOCALDOMAIN'

*kafka* will be the service name, that we configure later for the brokers and the Kafka clients. The hostname (for example
*kafka-0.localdomain* above) must be the FQDN of each Kafka broker. On this occasion, we can also create principals for
Kafka clients, for example consumers or producers, for example

    # kadmin.local -q 'addprinc -randkey kafka-producer@LOCALDOMAIN'

To create the keytab file in the */etc/security/keytabs* directory, it may be necessary to create that directory before:

    # mkdir -p /etc/security/keytabs

Then create the common keytab file and include the key for every service principal you have created before.
We name the keytab file *kafka-servers.keytab* here:

    # kadmin.local -q 'ktadd -k /etc/security/keytabs/kafka-servers.keytab kafka/kafka-0.localdomain@LOCALDOMAIN'
    # kadmin.local -q 'ktadd -k /etc/security/keytabs/kafka-servers.keytab kafka/kafka-1.localdomain@LOCALDOMAIN'
    # kadmin.local -q 'ktadd -k /etc/security/keytabs/kafka-servers.keytab kafka/kafka-2.localdomain@LOCALDOMAIN'

You can list the content of the keytab file with

    # klist -e -k -t /etc/security/keytabs/kafka-servers.keytab

Distribute the keytab file to all broker nodes. It can be placed in any directory that can be accessed by the
user under which Kafka is running. On the broker nodes we install it in *\<KAFKA_HOME\>/private/keytab/*. KAFKA_HOME
is the directory, which also contains the *config/*, *bin/*, and *libs/* folder. The directory must be created
before copying the file. Run following commands on every Kafka broker to transfer the keytabs file:

    $ mkdir -p <KAFKA_HOME>/private/keytab
    $ cd <KAFKA_HOME>/private/keytab
    $ scp root@zk-0.localdomain:/etc/security/keytabs/kafka-servers.keytab .

In the commands, replace KAFKA_HOME, and the hostname of your KDC (zookeeper) server.
After this step, the keytab file can be deleted from the KDC server.

As alternative, you can create the keytab file for every broker directly on each Kafka broker. For example, on host
kafka-0.localdomain you would run:

    $ mkdir -p <KAFKA_HOME>/private/keytab
    $ cd <KAFKA_HOME>/private/keytab
    $ kadmin -q 'ktadd -k ./kafka-servers.keytab kafka/kafka-0.localdomain@LOCALDOMAIN'

You will be prompted for the password of your admin principal. With this alternative, every broker gets an individual
keytab file for the service principal.

### Configure Kerberos

To configure Kerberos, for example the default realm and the settings for the realm, copy the previously
prepared file */etc/krb5.conf* from the zookeeper node (KDC) to all broker nodes to their
*/etc* directory. For example, on every Kafka host run following command:

    # scp root@zk-0.localdomain:/etc/krb5.conf /etc/

The file */etc/krb5.conf* is one of the default locations of the JVM's security provider to look for Kerberos settings.

### Configure JAAS for the broker nodes

On every broker, create a JAAS configuration file. You can use any filename for it. Here we create the JAAS config
in *\<KAFKA_HOME\>/config/kafka_server_jaas_krb5.conf*:

    KafkaServer {
        com.sun.security.auth.module.Krb5LoginModule required
        useKeyTab=true
        storeKey=true
        keyTab="/home/rolef/kafka/kafka_2.12-2.1.1/private/keytab/kafka-servers.keytab"
        principal="kafka/kafka-0.localdomain@LOCALDOMAIN";
    };

When Kafka runs on openJDK, use the `com.sun.security.auth.module.Krb5LoginModule` class as the login module.
When using IBM Java, use `com.ibm.security.auth.module.Krb5LoginModule` and the the parameters described in the
Javadoc of the [IBM security provider](https://www.ibm.com/support/knowledgecenter/SSYKE2_7.1.0/com.ibm.java.security.api.71.doc/jgss/com/ibm/security/auth/module/Krb5LoginModule.html).
Add the right location for the keytab file, and use the service principal for the node, where you are creating the config.
For example, on the node *kafka-1.localdomain* you would use the principal *kafka/kafka-1.localdomain@LOCALDOMAIN*.

Add the JAAS config file to the KAFKA_OPTS environment variable:

    $ export KAFKA_OPTS="-Djava.security.auth.login.config=/home/rolef/kafka/kafka_2.12-2.1.1/config/kafka_server_jaas_krb5.conf"

Make sure you add the right path to the previously created JAAS config file.
You can also add this environment variable to the *~/.bashrc* on every Kafka node.

### Configure the server properties

First, we are going to configure GSSAPI without SSL. Switching to GSSAPI over SSL later on demand is only little effort.
On every broker, edit *\<KAFKA_HOME\>/config/server.properties*. Ensure following:

- every broker has a unique *broker.id*.
- enter the right hostname (FQDN) for each broker in *listeners* and *advertised_listeners*.
- *sasl.kerberos.service.name* is the primary principal name that the Kafka brokers, for example the
  first part of the service principal *kafka/kafka-0.localdomain@LOCALDOMAIN*.

Properties for every broker's *server.properties*:

    broker.id=0
    sasl.enabled.mechanisms=GSSAPI
    sasl.kerberos.service.name=kafka
    sasl.mechanism.inter.broker.protocol=GSSAPI
    security.inter.broker.protocol=SASL_PLAINTEXT
    listeners=SASL_PLAINTEXT://kafka-0.localdomain:9093
    advertised.listeners=SASL_PLAINTEXT://kafka-0.localdomain:9093

When going to **SSL and GSSAPI**, change following properties:

    security.inter.broker.protocol=SASL_SSL
    listeners=SASL_SSL://kafka-0.localdomain:9093
    advertised.listeners=SASL_SSL://kafka-0.localdomain:9093

In addition, create keystores and a truststore (if the brokers do not trust the
certificates they present each other, and for Kafka clients) and create following
properties (give them the values of your environment) on every broker. Make sure
you replace the paths and password from the example below by your values:

    ssl.keystore.location=/home/rolef/kafka/kafka_2.12-2.1.1/private/ssl/kafka-0.localdomain.jks
    ssl.keystore.password=passw0rrd
    ssl.key.password=passw0rrd
    ssl.truststore.location=/home/rolef/kafka/kafka_2.12-2.1.1/private/ssl/truststore.jks
    ssl.truststore.password=passw0rrd
    ssl.protocol=TLSv1.2

The [Encryption with TLS](https://docs.confluent.io/5.5.2/kafka/authentication_ssl.html) section of the confluent
documentation describes all necessary steps.

### Start zookeeper and Kafka servers

Start the zookeeper with

    $ cd <KAFKA_HOME>
    $ bin/zookeeper-server-start.sh config/zookeeper.properties

Start all Kafka brokers with

    $ cd <KAFKA_HOME>
    $ bin/kafka-server-start.sh config/server.properties

Now you can create topics by issuing

    $ cd <KAFKA_HOME>
    $ bin/kafka-topics.sh --create --zookeeper zk-0.localdomain:2181 --replication-factor 3 --partitions 3  --topic your-topic-name

## Configure Kafka clients

Kafka clients need principals and keytab files for their prinicipals.
If you have not yet created principals for consumers and producers, login to the KDC and run as root:

    # kadmin.local
    kadmin.local:  addprinc -randkey kafka-producer@LOCALDOMAIN
    kadmin.local:  addprinc -randkey kafka-consumer@LOCALDOMAIN
    kadmin.local:  ktadd -k /tmp/kafka-producer@LOCALDOMAIN
    kadmin.local:  ktadd -k /tmp/kafka-consumer@LOCALDOMAIN
    kadmin.local:  quit

You are free in the primary principal names. You can also use the same principal for producers and consumers.
The commands above create principals and keytab files in */tmp*.
After creation, these files will be owned by root and will be readable by root only. Transfer the keytab files
to your Streams development project in a secure manner and delete them from the */tmp*
directory of the KDC host.

Follow [this article](https://ibmstreams.github.io/streamsx.kafka/docs/user/UsingKerberos/) from the toolkit
documentation to configure the Kafka toolkit operators to connect with a kerberized Kafka cluster.

