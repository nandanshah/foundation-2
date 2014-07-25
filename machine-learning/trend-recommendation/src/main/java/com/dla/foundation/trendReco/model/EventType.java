package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Map;

public class EventType implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int eventType;
	private String eventName;
	private Map<String, Integer> threshold;
	private int weight;

	public EventType(int eventType, String eventName,
			Map<String, Integer> threshold, int weight) {
		super();
		this.eventName = eventName;
		this.eventType = eventType;
		this.threshold = threshold;
		this.weight = weight;
	}

	public EventType(int eventid, String eventType, int weight) {
		this(eventid, eventType, null, weight);
	}

	public int getEventType() {
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
