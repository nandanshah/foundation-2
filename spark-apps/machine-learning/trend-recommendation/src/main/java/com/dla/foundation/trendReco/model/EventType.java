package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Map;

public class EventType implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String eventType;
	private double threshold;
	private int weight;

	
	public EventType(String eventType, int weight) {
		this(eventType,-1, weight);
	}
	
	public EventType(String eventType,
			double threshold, int weight) {
		super();
		
		this.eventType = eventType;
		this.threshold = threshold;
		this.weight = weight;
	}

	public String getEventType() {
		return eventType;
	}
	
	public double getThreshold() {
		return threshold;
	}

	public int getWeight() {
		return weight;
	}

}
