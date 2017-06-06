#!/bin/sh

alias st=streamtool

### Modify these if necessary ###
D=${STREAMS_DOMAIN_ID} # domain name
I=${STREAMS_INSTANCE_ID} # instance name
APP_CONFIG="kafka-test"

while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -b|--bootstrap)
    B="$2"
    ;;
    *)
    echo "Unrecognized option: $1"
    exit
    ;;
esac
shift
done

if [ -z ${B} ];
then
echo "Need to specify -b option to set bootstrap server(s)";
exit
fi

echo bootstrap.servers="${B}"

### DO NOT MODIFY ###
echo "Using domain: $D"
echo "Using instance: $I"

echo "Creating appConfig \"kafka-test\"..."
st mkappconfig -d $D -i $I --property bootstrap.servers=$B --property retries=30 ${APP_CONFIG}
st getappconfig -d $D -i $I ${APP_CONFIG}

echo "Creating properties file: \"etc\brokers.properties\"..."
mkdir -p etc
touch etc/brokers.properties
echo "bootstrap.servers=${B}" > etc/brokers.properties
echo "retries=30" >> etc/brokers.properties
echo "Done creating properties file!"
