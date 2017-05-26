package com.ibm.streamsx.kafka.properties;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public class MessageHubCredentials implements Serializable {
	private static final long serialVersionUID = 1L;

	@SerializedName("api_key")
	private String apiKey;

	@SerializedName("kafka_rest_url")
	private String kafkaRestUrl;

	@SerializedName("user")
	private String user;

	@SerializedName("password")
	private String password;

	@SerializedName("kafka_brokers_sasl")
	private String[] kafkaBrokersSasl;
	
	private MessageHubCredentials() { }

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String[] getKafkaBrokersSasl() {
		return kafkaBrokersSasl;
	}
}
