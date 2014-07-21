package com.dla.foundation.data.persistence.elasticsearch;



public class UserRecommendation {
	public String userid;
	public double socialScore;
	public double trendScore;
	public double popularScore;
	public double dlaRecoScore;
	public double editorRecoScore;
	public double otherTypeScore;
	
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
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
	public double getDlaRecoScore() {
		return dlaRecoScore;
	}
	public void setDlaRecoScore(double dlaRecoScore) {
		this.dlaRecoScore = dlaRecoScore;
	}
	public double getEditorRecoScore() {
		return editorRecoScore;
	}
	public void setEditorRecoScore(double editorRecoScore) {
		this.editorRecoScore = editorRecoScore;
	}
	
	
	public double getOtherTypeScore() {
		return otherTypeScore;
	}
	public void setOtherTypeScore(double otherTypeScore) {
		this.otherTypeScore = otherTypeScore;
	}
	@Override
	public String toString() {
		return "UserRecommendation [dlaRecoScore=" + dlaRecoScore
				+ ", editorRecoScore=" + editorRecoScore + ", popularScore="
				+ popularScore + ", socialScore=" + socialScore
				+ ", trendScore=" + trendScore + ", userid=" + userid + "]";
	}
	
	
	

}
