package com.dla.foundation.fis.eo.entities;

public enum SearchType {

	TITLE_MATCH("title_match"), 
	FUZZY_TITLE_MATCH("fuzzy_title_match"), 
	EXTENDED_SEARCH("extended_search"), 
	ALTERNATIVE_suggestion("alternative_suggestion");

	private String searchType;

	private SearchType(String searchType) {
		this.searchType = searchType;
	}

	public String getDeviceType() {
		return searchType;
	}
}
