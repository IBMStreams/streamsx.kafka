package com.ibm.streamsx.kafka.clients;

import java.io.Serializable;
import java.util.Base64;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;

public abstract class AbstractKafkaClient {

	private static final Logger logger = Logger.getLogger(AbstractKafkaClient.class);
	
	public <T> String getSerializer(Class<T> clazz) throws Exception {
		if(clazz.equals(String.class) || clazz.equals(RString.class)) {
			return StringSerializer.class.getCanonicalName();
		} else if(clazz.equals(Long.class)) {
			return LongSerializer.class.getCanonicalName();
		} else if(clazz.equals(Double.class)) {
			return DoubleSerializer.class.getCanonicalName();
		} else if(clazz.equals(Blob.class)) {
			return ByteArraySerializer.class.getCanonicalName();
		} else if(clazz.equals(Integer.class) || clazz.equals(Short.class) || clazz.equals(Byte.class)) {
			return IntegerSerializer.class.getCanonicalName();
		} else {
			throw new Exception("Unable to find serializer for: " + clazz.toString()); //$NON-NLS-1$
		}
	}

	public String inferDeserializerFromSerializer(String serializerClassName) throws Exception {
		if(serializerClassName.equals(StringSerializer.class.getCanonicalName())) {
			return StringDeserializer.class.getCanonicalName();
		} else if(serializerClassName.equals(LongSerializer.class.getCanonicalName())) {
			return LongDeserializer.class.getCanonicalName();
		} else if(serializerClassName.equals(DoubleSerializer.class.getCanonicalName())) {
			return DoubleDeserializer.class.getCanonicalName();
		} else if(serializerClassName.equals(ByteArraySerializer.class.getCanonicalName())) {
			return ByteArrayDeserializer.class.getCanonicalName();
		} else if(serializerClassName.equals(IntegerSerializer.class.getCanonicalName())) {
			return IntegerDeserializer.class.getCanonicalName();
		} else {
			throw new Exception("Unable to infer deserializer from serializer: " + serializerClassName); //$NON-NLS-1$
		}
	}
	
	public <T> String getDeserializer(Class<T> clazz) throws Exception {
		if(clazz.equals(String.class) || clazz.equals(RString.class)) {
			return StringDeserializer.class.getCanonicalName();
		} else if(clazz.equals(Long.class)) {
			return LongDeserializer.class.getCanonicalName();
		} else if(clazz.equals(Double.class)) {
			return DoubleDeserializer.class.getCanonicalName();
		} else if(clazz.equals(Blob.class)) {
			return ByteArrayDeserializer.class.getCanonicalName();
		} else if(clazz.equals(Integer.class) || clazz.equals(Short.class) || clazz.equals(Byte.class)) {
			return IntegerDeserializer.class.getCanonicalName();
		} else {
			throw new Exception("Unable to find deserializer for: " + clazz.toString()); //$NON-NLS-1$
		}
	}
	
	protected String serializeObject(Serializable obj) {
		return new String(Base64.getEncoder().encode(SerializationUtils.serialize(obj)));
	}
	
	protected String getRandomId(String prefix) {
		String id = prefix + RandomStringUtils.randomAlphanumeric(17); 
		logger.debug("Random id=" + id); //$NON-NLS-1$
		return id;
	}
}
