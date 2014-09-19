package com.dla.foundation.socialReco.model;
import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class UserScore  implements Serializable{


	public  static String DELIM = "####";
	/**
	 * 
	 */
	private static final long serialVersionUID = -4635904137451689023L;
	private String tenantId;
	private String regionId;
	private String userId;
	private Map<String, Double> movieScore;
	private Map<String, Map<String, Double>> eventTypeAggregate;
	
	public UserScore(String tenantId, String regionId, String userId, Map<String, Double> movieScore,
			Map<String, Map<String, Double>> eventTypeAggregate) {

		this.tenantId = tenantId;
		this.regionId = regionId;
		this.userId = userId;
		this.movieScore = movieScore;
		this.eventTypeAggregate = eventTypeAggregate;
		
	}
	
	public UserScore() {
		super();
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getTenantId() {
		return tenantId;
	}

	public String getRegionId() {
		return regionId;
	}

	public String getUserId() {
		return userId;
	}

	public Map<String, Double> getMovieScore() {
		return movieScore;
	}

	public void setMovieScore(Map<String, Double> movieScore) {
		this.movieScore = movieScore;
	}
	
	public Map<String, Map<String, Double>> getEventTypeAggregate() {
		return eventTypeAggregate;
	}

	public void setEventTypeAggregate(Map<String, Map<String, Double>> eventTypeAggregate) {
		this.eventTypeAggregate = eventTypeAggregate;
	}

	@Override
	public String toString() {
		return getTenantId()+DELIM+getRegionId()+DELIM+getUserId()+DELIM+getMovieScore()
				+DELIM+getEventTypeAggregate();
	}

}
