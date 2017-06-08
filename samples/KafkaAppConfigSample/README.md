# KafkaSample

This sample demonstrates how to specify the application configuration name that contains the Kafka properties. Each property in the application configuration will be loaded as a Kafka property. 

### Setup

To run this sample, you need to create an application configuration called "kafka-sample". The app config needs to contain a property named "bootstrap.servers" that specifies the Kafka brokers you want to connect to. This can be done at the command-line using the following commands: 

```
streamtool mkappconfig -d <your_domain_name> -i <your_instance_name> kafka-sample
streamtool chappconfig -d <your_domain_name> -i <your_instance_name> --property bootstrap.servers=<your_brokers_here> kafka-sample
``` 

**TIP #1:** The above commands can be combined into a single command by specifying the `--property` flag in `mkappconfig`. Here is an example: 

```
streamtool mkappconfig -d StreamsDomain -i StreamsInstance --property bootstrap.servers=mybroker:9091 kafka-sample
```

**TIP #2:** Application configurations can also be created using the Streams Console. More information on application configurations can be found here: [Creating application configuration objects to securely store data](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.1/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html)

