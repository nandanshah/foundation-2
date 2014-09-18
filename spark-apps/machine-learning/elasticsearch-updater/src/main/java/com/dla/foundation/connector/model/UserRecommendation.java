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
	private double popularityScore;
	private double fpScore;
	private double newReleaseScore;
	private double recoByFoundationScore;
	private String trendScoreReason;
	private String socialScoreReason;
	private String popularityScoreReason;
	private String fpScoreReason;
	private String recoByFoundationReason;
	private String newReleaseScoreReason;
	
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
	public double getPopularityScore() {
		return popularityScore;
	}
	public void setPopularityScore(double popularityScore) {
		this.popularityScore = popularityScore;
	}
	public double getFpScore() {
		return fpScore;
	}
	public void setFpScore(double fpScore) {
		this.fpScore = fpScore;
	}
	public double getNewReleaseScore() {
		return newReleaseScore;
	}
	public void setNewReleaseScore(double newReleaseScore) {
		this.newReleaseScore = newReleaseScore;
	}
	public double getRecoByFoundationScore() {
		return recoByFoundationScore;
	}
	public void setRecoByFoundationScore(double recoByFoundationScore) {
		this.recoByFoundationScore = recoByFoundationScore;
	}

	public String getTrendScoreReason() {
		return trendScoreReason;
	}
	public void setTrendScoreReason(String trendScoreReason) {
		this.trendScoreReason = trendScoreReason;
	}
	public String getSocialScoreReason() {
		return socialScoreReason;
	}
	public void setSocialScoreReason(String socialScoreReason) {
		this.socialScoreReason = socialScoreReason;
	}
	public String getPopularityScoreReason() {
		return popularityScoreReason;
	}
	public void setPopularityScoreReason(String popularityScoreReason) {
		this.popularityScoreReason = popularityScoreReason;
	}
	public String getFpScoreReason() {
		return fpScoreReason;
	}
	public void setFpScoreReason(String fpScoreReason) {
		this.fpScoreReason = fpScoreReason;
	}
	public String getRecoByFoundationReason() {
		return recoByFoundationReason;
	}
	public void setRecoByFoundationReason(String recoByFoundationReason) {
		System.out.println("recoByFoundation"+recoByFoundationReason);
		this.recoByFoundationReason = recoByFoundationReason;
	}
	@Override
	public String toString() {
		return "UserRecommendation [profileId=" + profileId + ", mediaItemId="
				+ mediaItemId + ", socialScore=" + socialScore
				+ ", trendScore=" + trendScore + ", popularityScore="
				+ popularityScore + ", fpScore=" + fpScore + ", newReleaseScore="
				+ newReleaseScore + ", recoByFoundationScore=" + recoByFoundationScore
				+ ", newReleaseScoreReason=" + newReleaseScoreReason +", trendScoreReason=" + trendScoreReason + ", socialScoreReason="
				+ socialScoreReason + ", popularityScoreReason=" + popularityScoreReason
				+ ", fpScoreReason=" + fpScoreReason + ", recoByFoundationReason="
				+ recoByFoundationReason + "]";
	}
	public String getNewReleaseScoreReason() {
		return newReleaseScoreReason;
	}
	public void setNewReleaseScoreReason(String newReleaseScoreReason) {
		this.newReleaseScoreReason = newReleaseScoreReason;
	}
	
}
