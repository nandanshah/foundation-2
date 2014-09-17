package com.dla.foundation.useritemreco.model;

import java.io.Serializable;

public class Score implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4638325287316572315L;

	private String type;
	private double score = -1;
	private String scoreReason;

	public Score(String scoreReason, double score) {
		this.scoreReason = scoreReason;
		this.score = score;
	}

	public Score() {
		super();
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getScoreReason() {
		return scoreReason;
	}

	public void setScoreReason(String scoreReason) {
		this.scoreReason = scoreReason;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
