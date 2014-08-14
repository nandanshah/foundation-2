package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Map;

public class EventType implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String eventType;
	private String eventName;
	private Map<String, Integer> threshold;
	private int weight;

	
	public EventType(String eventType, int weight) {
		this(eventType, null, weight);
	}

	
	public EventType(String eventType,
			Map<String, Integer> threshold, int weight) {
		super();
		
		this.eventType = eventType;
		this.threshold = threshold;
		this.weight = weight;
	}

	public String getEventType() {
		return eventType;
	}

	public String getEventName() {
		return eventName;
	}

	public Map<String, Integer> getThreshold() {
		return threshold;
	}

	public int getWeight() {
		return weight;
	}

}
