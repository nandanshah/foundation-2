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
	private Map<String, Double> weight;

	
	public EventType(String eventType, Map<String, Double> weight) {
		this(eventType,-1, weight);
	}
	
	public EventType(String eventType,
			double threshold, Map<String, Double> weight) {
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

	public Map<String, Double> getWeight() {
		return weight;
	}

}
