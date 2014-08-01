package com.dla.foundation.fis.eo.entities;

public enum ImpressionSource {

	CATALOG_VIEW("catalog_view"), ARTICLE_VIEW("article_view"), SEARCH_RESULT(
			"search_result"), SIMILAR_ITEMS("similar_items"), POST_PLAY_SUGGESTIONS(
			"post_play_suggestions"), EMAIL_LINK("email_link"), SEO_LINK(
			"seo_link"), DIRECT_LINK("direct_link"), OTHER("other");

	private String impressionSource;

	private ImpressionSource(String impressionSource) {
		this.impressionSource = impressionSource;
	}

}
