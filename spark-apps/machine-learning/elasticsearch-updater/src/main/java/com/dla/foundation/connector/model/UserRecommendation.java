package com.dla.foundation.connector.model;

import java.io.Serializable;



public class UserRecommendation extends ESEntity implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2898274449817898426L;
	private String userid;
	private String parentItemId;
	private double socialScore;
	private double trendScore;
	private double popularScore;
	private double fpScore;
	private double newScore;
	private double recoByFoundation;
	private String trendreason;
	private String socialreason;
	private String popularityreason;
	private String fpreason;
	private String recoByfoundationreason;
	private String newreason;
	
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getParentItemId() {
		return parentItemId;
	}
	public void setParentItemId(String parentItemId) {
		this.parentItemId = parentItemId;
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
		return trendreason;
	}
	public void setTrendreason(String trendreason) {
		this.trendreason = trendreason;
	}
	public String getSocialreason() {
		return socialreason;
	}
	public void setSocialreason(String socialreason) {
		this.socialreason = socialreason;
	}
	public String getPopularityreason() {
		return popularityreason;
	}
	public void setPopularityreason(String popularityreason) {
		this.popularityreason = popularityreason;
	}
	public String getFpreason() {
		return fpreason;
	}
	public void setFpreason(String fpreason) {
		this.fpreason = fpreason;
	}
	public String getRecoByfoundationreason() {
		return recoByfoundationreason;
	}
	public void setRecoByfoundationreason(String recoByfoundationreason) {
		this.recoByfoundationreason = recoByfoundationreason;
	}
	@Override
	public String toString() {
		return "UserRecommendation [userid=" + userid + ", parentItemId="
				+ parentItemId + ", socialScore=" + socialScore
				+ ", trendScore=" + trendScore + ", popularScore="
				+ popularScore + ", fpScore=" + fpScore + ", newScore="
				+ newScore + ", recoByFoundation=" + recoByFoundation
				+ ", trendreason=" + trendreason + ", socialreason="
				+ socialreason + ", popularityreason=" + popularityreason
				+ ", fpreason=" + fpreason + ", recoByfoundationreason="
				+ recoByfoundationreason + "]";
	}
	
}
