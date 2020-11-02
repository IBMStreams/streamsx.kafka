.PHONY: all clean

COMPOSITE_NAME = KafkaAvroSample
SPL_NAMESPACE = com.ibm.streamsx.kafka.sample
SPL_MAIN_COMPOSITE = $(SPL_NAMESPACE)::$(COMPOSITE_NAME)
OUTPUT_DIR = output/$(SPL_NAMESPACE).$(COMPOSITE_NAME)

SPLC_FLAGS = -a
SPLC = $(STREAMS_INSTALL)/bin/sc

ifeq ($(STREAMSX_AVRO_TOOLKIT),)
  ifneq ($(realpath ../../../streamsx.avro/com.ibm.streamsx.avro),)
    STREAMSX_AVRO_TOOLKIT = ../../../streamsx.avro/com.ibm.streamsx.avro
  endif
  ifneq ($(realpath $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.avro),)
    STREAMSX_AVRO_TOOLKIT = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.avro
  endif
endif

ifdef STREAMS_BUILDSERVICE
  SPL_PATH = ../toolkits:$(STREAMS_INSTALL)/toolkits
else
  SPL_PATH = ../..:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.kafka:$(STREAMSX_AVRO_TOOLKIT)
endif

all:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-directory $(OUTPUT_DIR) -t $(SPL_PATH); \

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-directory $(OUTPUT_DIR); \
	rm -rf $(OUTPUT_DIR)
	rm -rf doc
