package com.dla.foundation.elasticsearch.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "cast", "crew" })
public class Credits {

	/**
     * 
     */
	@JsonProperty("cast")
	private List<Cast> cast = new ArrayList<Cast>();
	/**
     * 
     */
	@JsonProperty("crew")
	private List<Crew> crew = new ArrayList<Crew>();
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
     * 
     */
	@JsonProperty("cast")
	public List<Cast> getCast() {
		return cast;
	}

	/**
     * 
     */
	@JsonProperty("cast")
	public void setCast(List<Cast> cast) {
		this.cast = cast;
	}

	/**
     * 
     */
	@JsonProperty("crew")
	public List<Crew> getCrew() {
		return crew;
	}

	/**
     * 
     */
	@JsonProperty("crew")
	public void setCrew(List<Crew> crew) {
		this.crew = crew;
	}

	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

	// get cast members below specified order
	public List<Cast> getCastByOrder(int order) {
		List<Cast> castMembers = new ArrayList<Cast>();
		for (Cast castMember : this.getCast()) {
			if (castMember.getOrder() < order) {
				castMembers.add(castMember);
			}
		}
		return castMembers;
	}

	public List<Crew> getCrewByDepartment(String department) {
		List<Crew> crewMembers = new ArrayList<Crew>();
		for (Crew crewMember : this.getCrew()) {
			if (crewMember.getDepartment().equalsIgnoreCase(department)) {
				crewMembers.add(crewMember);
			}
		}
		return crewMembers;
	}

}
