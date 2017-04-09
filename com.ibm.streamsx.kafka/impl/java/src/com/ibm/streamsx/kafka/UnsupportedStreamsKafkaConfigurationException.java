/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.kafka;

public class UnsupportedStreamsKafkaConfigurationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public UnsupportedStreamsKafkaConfigurationException(String message){
		super(message);
	}
}
