
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
@JsonPropertyOrder({
    "adult",
    "backdrop_path",
    "belongs_to_collection",
    "budget",
    "credits",
    "genres",
    "homepage",
    "id",
    "imdb_id",
    "original_title",
    "overview",
    "popularity",
    "poster_path",
    "production_companies",
    "production_countries",
    "release_date",
    "revenue",
    "runtime",
    "spoken_languages",
    "status",
    "tagline",
    "title",
    "vote_average",
    "vote_count"
})
public class Movie {

    /**
     * 
     */
    @JsonProperty("adult")
    private Boolean adult;
    /**
     * 
     */
    @JsonProperty("backdrop_path")
    private String backdrop_path;
    /**
     * 
     */
    @JsonProperty("belongs_to_collection")
    private Belongs_to_collection belongs_to_collection;
    /**
     * 
     */
    @JsonProperty("budget")
    private Double budget;
    /**
     * 
     */
    @JsonProperty("credits")
    private Credits credits;
    /**
     * 
     */
    @JsonProperty("genres")
    private List<Genre> genres = new ArrayList<Genre>();
    /**
     * 
     */
    @JsonProperty("homepage")
    private String homepage;
    /**
     * 
     */
    @JsonProperty("id")
    private Double id;
    /**
     * 
     */
    @JsonProperty("imdb_id")
    private String imdb_id;
    /**
     * 
     */
    @JsonProperty("original_title")
    private String original_title;
    /**
     * 
     */
    @JsonProperty("overview")
    private String overview;
    /**
     * 
     */
    @JsonProperty("popularity")
    private String popularity;
    /**
     * 
     */
    @JsonProperty("poster_path")
    private String poster_path;
    /**
     * 
     */
    @JsonProperty("production_companies")
    private List<Production_company> production_companies = new ArrayList<Production_company>();
    /**
     * 
     */
    @JsonProperty("production_countries")
    private List<Production_country> production_countries = new ArrayList<Production_country>();
    /**
     * 
     */
    @JsonProperty("release_date")
    private String release_date;
    /**
     * 
     */
    @JsonProperty("revenue")
    private Double revenue;
    /**
     * 
     */
    @JsonProperty("runtime")
    private Double runtime;
    /**
     * 
     */
    @JsonProperty("spoken_languages")
    private List<Spoken_language> spoken_languages = new ArrayList<Spoken_language>();
    /**
     * 
     */
    @JsonProperty("status")
    private String status;
    /**
     * 
     */
    @JsonProperty("tagline")
    private String tagline;
    /**
     * 
     */
    @JsonProperty("title")
    private String title;
    /**
     * 
     */
    @JsonProperty("vote_average")
    private Double vote_average;
    /**
     * 
     */
    @JsonProperty("vote_count")
    private Double vote_count;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * 
     */
    @JsonProperty("adult")
    public Boolean getAdult() {
        return adult;
    }

    /**
     * 
     */
    @JsonProperty("adult")
    public void setAdult(Boolean adult) {
        this.adult = adult;
    }

    /**
     * 
     */
    @JsonProperty("backdrop_path")
    public String getBackdrop_path() {
        return backdrop_path;
    }

    /**
     * 
     */
    @JsonProperty("backdrop_path")
    public void setBackdrop_path(String backdrop_path) {
        this.backdrop_path = backdrop_path;
    }

    /**
     * 
     */
    @JsonProperty("belongs_to_collection")
    public Belongs_to_collection getBelongs_to_collection() {
        return belongs_to_collection;
    }

    /**
     * 
     */
    @JsonProperty("belongs_to_collection")
    public void setBelongs_to_collection(Belongs_to_collection belongs_to_collection) {
        this.belongs_to_collection = belongs_to_collection;
    }

    /**
     * 
     */
    @JsonProperty("budget")
    public Double getBudget() {
        return budget;
    }

    /**
     * 
     */
    @JsonProperty("budget")
    public void setBudget(Double budget) {
        this.budget = budget;
    }

    /**
     * 
     */
    @JsonProperty("credits")
    public Credits getCredits() {
        return credits;
    }

    /**
     * 
     */
    @JsonProperty("credits")
    public void setCredits(Credits credits) {
        this.credits = credits;
    }

    /**
     * 
     */
    @JsonProperty("genres")
    public List<Genre> getGenres() {
        return genres;
    }

    /**
     * 
     */
    @JsonProperty("genres")
    public void setGenres(List<Genre> genres) {
        this.genres = genres;
    }

    /**
     * 
     */
    @JsonProperty("homepage")
    public String getHomepage() {
        return homepage;
    }

    /**
     * 
     */
    @JsonProperty("homepage")
    public void setHomepage(String homepage) {
        this.homepage = homepage;
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
    @JsonProperty("imdb_id")
    public String getImdb_id() {
        return imdb_id;
    }

    /**
     * 
     */
    @JsonProperty("imdb_id")
    public void setImdb_id(String imdb_id) {
        this.imdb_id = imdb_id;
    }

    /**
     * 
     */
    @JsonProperty("original_title")
    public String getOriginal_title() {
        return original_title;
    }

    /**
     * 
     */
    @JsonProperty("original_title")
    public void setOriginal_title(String original_title) {
        this.original_title = original_title;
    }

    /**
     * 
     */
    @JsonProperty("overview")
    public String getOverview() {
        return overview;
    }

    /**
     * 
     */
    @JsonProperty("overview")
    public void setOverview(String overview) {
        this.overview = overview;
    }

    /**
     * 
     */
    @JsonProperty("popularity")
    public String getPopularity() {
        return popularity;
    }

    /**
     * 
     */
    @JsonProperty("popularity")
    public void setPopularity(String popularity) {
        this.popularity = popularity;
    }

    /**
     * 
     */
    @JsonProperty("poster_path")
    public String getPoster_path() {
        return poster_path;
    }

    /**
     * 
     */
    @JsonProperty("poster_path")
    public void setPoster_path(String poster_path) {
        this.poster_path = poster_path;
    }

    /**
     * 
     */
    @JsonProperty("production_companies")
    public List<Production_company> getProduction_companies() {
        return production_companies;
    }

    /**
     * 
     */
    @JsonProperty("production_companies")
    public void setProduction_companies(List<Production_company> production_companies) {
        this.production_companies = production_companies;
    }

    /**
     * 
     */
    @JsonProperty("production_countries")
    public List<Production_country> getProduction_countries() {
        return production_countries;
    }

    /**
     * 
     */
    @JsonProperty("production_countries")
    public void setProduction_countries(List<Production_country> production_countries) {
        this.production_countries = production_countries;
    }

    /**
     * 
     */
    @JsonProperty("release_date")
    public String getRelease_date() {
        return release_date;
    }

    /**
     * 
     */
    @JsonProperty("release_date")
    public void setRelease_date(String release_date) {
        this.release_date = release_date;
    }

    /**
     * 
     */
    @JsonProperty("revenue")
    public Double getRevenue() {
        return revenue;
    }

    /**
     * 
     */
    @JsonProperty("revenue")
    public void setRevenue(Double revenue) {
        this.revenue = revenue;
    }

    /**
     * 
     */
    @JsonProperty("runtime")
    public Double getRuntime() {
        return runtime;
    }

    /**
     * 
     */
    @JsonProperty("runtime")
    public void setRuntime(Double runtime) {
        this.runtime = runtime;
    }

    /**
     * 
     */
    @JsonProperty("spoken_languages")
    public List<Spoken_language> getSpoken_languages() {
        return spoken_languages;
    }

    /**
     * 
     */
    @JsonProperty("spoken_languages")
    public void setSpoken_languages(List<Spoken_language> spoken_languages) {
        this.spoken_languages = spoken_languages;
    }

    /**
     * 
     */
    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    /**
     * 
     */
    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * 
     */
    @JsonProperty("tagline")
    public String getTagline() {
        return tagline;
    }

    /**
     * 
     */
    @JsonProperty("tagline")
    public void setTagline(String tagline) {
        this.tagline = tagline;
    }

    /**
     * 
     */
    @JsonProperty("title")
    public String getTitle() {
        return title;
    }

    /**
     * 
     */
    @JsonProperty("title")
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * 
     */
    @JsonProperty("vote_average")
    public Double getVote_average() {
        return vote_average;
    }

    /**
     * 
     */
    @JsonProperty("vote_average")
    public void setVote_average(Double vote_average) {
        this.vote_average = vote_average;
    }

    /**
     * 
     */
    @JsonProperty("vote_count")
    public Double getVote_count() {
        return vote_count;
    }

    /**
     * 
     */
    @JsonProperty("vote_count")
    public void setVote_count(Double vote_count) {
        this.vote_count = vote_count;
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
