
package com.dla.foundation.elasticsearch.entities;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "cast_id",
    "character",
    "credit_id",
    "id",
    "name",
    "order",
    "profile_path"
})
public class Cast {

    /**
     * 
     */
    @JsonProperty("cast_id")
    private Double cast_id;
    /**
     * 
     */
    @JsonProperty("character")
    private String character;
    /**
     * 
     */
    @JsonProperty("credit_id")
    private String credit_id;
    /**
     * 
     */
    @JsonProperty("id")
    private Double id;
    /**
     * 
     */
    @JsonProperty("name")
    private String name;
    /**
     * 
     */
    @JsonProperty("order")
    private Double order;
    /**
     * 
     */
    @JsonProperty("profile_path")
    private String profile_path;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * 
     */
    @JsonProperty("cast_id")
    public Double getCast_id() {
        return cast_id;
    }

    /**
     * 
     */
    @JsonProperty("cast_id")
    public void setCast_id(Double cast_id) {
        this.cast_id = cast_id;
    }

    /**
     * 
     */
    @JsonProperty("character")
    public String getCharacter() {
        return character;
    }

    /**
     * 
     */
    @JsonProperty("character")
    public void setCharacter(String character) {
        this.character = character;
    }

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
    @JsonProperty("order")
    public Double getOrder() {
        return order;
    }

    /**
     * 
     */
    @JsonProperty("order")
    public void setOrder(Double order) {
        this.order = order;
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
