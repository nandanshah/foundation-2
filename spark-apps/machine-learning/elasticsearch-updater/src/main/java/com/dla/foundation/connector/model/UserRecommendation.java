package com.dla.foundation.connector.model;

import java.io.Serializable;



public class UserRecommendation extends ESEntity implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2898274449817898426L;
	public String userid;
	public String parentItemId;
	public double socialScore;
	public double trendScore;
	public double popularScore;
	public double fpScore;
	public double newScore;
	public double recoByFoundation;
	
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

	@Override
	public String toString() {
		return "UserRecommendation [userid=" + userid + ", socialScore="
				+ socialScore + ", trendScore=" + trendScore
				+ ", popularScore=" + popularScore + ", fpScore=" + fpScore
				+ ", newScore=" + newScore + ", recoByFoundation="
				+ recoByFoundation + "]";
	}
	
}
