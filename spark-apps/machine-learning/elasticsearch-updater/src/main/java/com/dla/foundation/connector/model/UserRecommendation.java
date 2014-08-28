package com.dla.foundation.connector.model;

import java.io.Serializable;



public class UserRecommendation extends ESEntity implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2898274449817898426L;
	private String profileId;
	private String mediaItemId;
	private int enabled;
	private double socialScore;
	private double trendScore;
	private double popularScore;
	private double fpScore;
	private double newScore;
	private double recoByFoundation;
	private String trendReason;
	private String socialReason;
	private String popularityReason;
	private String fpReason;
	private String recoByfoundationReason;
	private String newReason;
	
	public String getprofileId() {
		return profileId;
	}
	public void setprofileId(String profileId) {
		this.profileId = profileId;
	}
	public String getmediaItemId() {
		return mediaItemId;
	}
	public void setmediaItemId(String mediaItemId) {
		this.mediaItemId = mediaItemId;
	}
	
	public int getEnabled() {
		return enabled;
	}
	public void setEnabled(int enabled) {
		this.enabled = enabled;
	}
	
	public double getSocialScore() {
		return socialScore;
	}
	public void setSocialScore(double socialScore) {
		this.socialScore = socialScore;
	}
	public double getTrendScore() {
		return trendScore;
	}
	public void setTrendScore(double trendScore) {
		this.trendScore = trendScore;
	}
	public double getPopularScore() {
		return popularScore;
	}
	public void setPopularScore(double popularScore) {
		this.popularScore = popularScore;
	}
	public double getFpScore() {
		return fpScore;
	}
	public void setFpScore(double fpScore) {
		this.fpScore = fpScore;
	}
	public double getNewScore() {
		return newScore;
	}
	public void setNewScore(double newScore) {
		this.newScore = newScore;
	}
	public double getRecoByFoundation() {
		return recoByFoundation;
	}
	public void setRecoByFoundation(double recoByFoundation) {
		this.recoByFoundation = recoByFoundation;
	}

	public String getTrendreason() {
		return trendReason;
	}
	public void setTrendreason(String trendreason) {
		this.trendReason = trendreason;
	}
	public String getSocialreason() {
		return socialReason;
	}
	public void setSocialreason(String socialreason) {
		this.socialReason = socialreason;
	}
	public String getPopularityreason() {
		return popularityReason;
	}
	public void setPopularityreason(String popularityreason) {
		this.popularityReason = popularityreason;
	}
	public String getFpreason() {
		return fpReason;
	}
	public void setFpreason(String fpreason) {
		this.fpReason = fpreason;
	}
	public String getRecoByfoundationreason() {
		return recoByfoundationReason;
	}
	public void setRecoByfoundationreason(String recoByfoundationreason) {
		System.out.println("recoByFoundation"+recoByfoundationreason);
		this.recoByfoundationReason = recoByfoundationreason;
	}
	@Override
	public String toString() {
		return "UserRecommendation [profileId=" + profileId + ", mediaItemId="
				+ mediaItemId + ", socialScore=" + socialScore
				+ ", trendScore=" + trendScore + ", popularScore="
				+ popularScore + ", fpScore=" + fpScore + ", newScore="
				+ newScore + ", recoByFoundation=" + recoByFoundation
				+ ", trendReason=" + trendReason + ", socialReason="
				+ socialReason + ", popularityReason=" + popularityReason
				+ ", fpReason=" + fpReason + ", recoByfoundationReason="
				+ recoByfoundationReason + "]";
	}
	public String getNewReason() {
		return newReason;
	}
	public void setNewReason(String newReason) {
		this.newReason = newReason;
	}
	
}
