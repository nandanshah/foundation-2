package com.dla.foundation.elasticsearch.entities;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "credit_id", "department", "id", "job", "name",
		"profile_path" })
public class Crew {

	/**
     * 
     */
	@JsonProperty("credit_id")
	private String credit_id;
	/**
     * 
     */
	@JsonProperty("department")
	private String department;
	/**
     * 
     */
	@JsonProperty("id")
	private Double id;
	/**
     * 
     */
	@JsonProperty("job")
	private String job;
	/**
     * 
     */
	@JsonProperty("name")
	private String name;
	/**
     * 
     */
	@JsonProperty("profile_path")
	private String profile_path;
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
     * 
     */
	@JsonProperty("credit_id")
	public String getCredit_id() {
		return credit_id;
	}

	/**
     * 
     */
	@JsonProperty("credit_id")
	public void setCredit_id(String credit_id) {
		this.credit_id = credit_id;
	}

	/**
     * 
     */
	@JsonProperty("department")
	public String getDepartment() {
		return department;
	}

	/**
     * 
     */
	@JsonProperty("department")
	public void setDepartment(String department) {
		this.department = department;
	}

	/**
     * 
     */
	@JsonProperty("id")
	public Double getId() {
		return id;
	}

	/**
     * 
     */
	@JsonProperty("id")
	public void setId(Double id) {
		this.id = id;
	}

	/**
     * 
     */
	@JsonProperty("job")
	public String getJob() {
		return job;
	}

	/**
     * 
     */
	@JsonProperty("job")
	public void setJob(String job) {
		this.job = job;
	}

	/**
     * 
     */
	@JsonProperty("name")
	public String getName() {
		return name;
	}

	/**
     * 
     */
	@JsonProperty("name")
	public void setName(String name) {
		this.name = name;
	}

	/**
     * 
     */
	@JsonProperty("profile_path")
	public String getProfile_path() {
		return profile_path;
	}

	/**
     * 
     */
	@JsonProperty("profile_path")
	public void setProfile_path(String profile_path) {
		this.profile_path = profile_path;
	}

	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

}
