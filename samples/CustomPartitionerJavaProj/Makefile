.PHONY: all clean

JAVAC = $(STREAMS_INSTALL)/java/bin/javac
JAR = $(STREAMS_INSTALL)/java/bin/jar
CLASSPATH = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.kafka/opt/downloaded/*:../../opt/downloaded/*:../../com.ibm.streamsx.kafka/opt/downloaded/*

all:
	if [ -x ../../gradlew ]; then \
	   ../../gradlew build; \
	else \
	  mkdir -p build/classes/main; \
	  mkdir -p build/libs; \
	  $(JAVAC) -verbose -sourcepath src -d build/classes/main -classpath $(CLASSPATH) src/main/java/com/ibm/streamsx/kafka/samples/partitioner/*.java ; \
	  $(JAR) -cvf build/libs/custom-partitioner.jar -C build/classes/main com; \
	fi

clean:
	if [ -x ../../gradlew ]; then \
	  ../../gradlew clean; \
	else \
	  rm -rf build; \
	fi
