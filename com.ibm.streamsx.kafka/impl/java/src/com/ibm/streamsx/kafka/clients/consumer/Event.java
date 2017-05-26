package com.ibm.streamsx.kafka.clients.consumer;

public class Event {

	public static enum EventType {
		START_POLLING,
		STOP_POLLING,
		CHECKPOINT,
		RESET,
		RESET_TO_INIT,
		SHUTDOWN;
	};
	
	private EventType eventType;
	private Object data;
	
	public Event(EventType eventType, Object data) {
		this.eventType = eventType;
		this.data = data;
	}
	
	public Object getData() {
		return data;
	}

	public EventType getEventType() {
		return eventType;
	}
}
