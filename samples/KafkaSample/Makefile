# Copyright (C)2016, International Business Machines Corporation and  
# others. All Rights Reserved.                        
.PHONY: all clean

STREAMS_KAFKA_TOOLKIT ?= ../../com.ibm.streamsx.kafka:$(STREAMS_STUDIO_SPL_PATH)

SPLC_FLAGS ?= -a -t ${STREAMS_KAFKA_TOOLKIT}

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = com.ibm.streamsx.kafka.sample::KafkaSample
OUTPUT_DIR = output

all: 
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(OUTPUT_DIR) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)


