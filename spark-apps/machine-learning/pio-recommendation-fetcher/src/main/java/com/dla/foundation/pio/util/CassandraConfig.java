package com.dla.foundation.pio.util;

import java.io.Serializable;

public class CassandraConfig implements Serializable {
	public final String recommendationUpdateQuery;
	public final String recommendationKeySpace;
	public final String recommendationColFamily;
	public final String recommendationPrimaryKey;
	public final String profileKeySpace;
	public final String profileColFamily;
	public final String pageRowSize;
	public final String profilePrimaryKey;

	public CassandraConfig() {
		this(null, null, null, null, null, null, null, null);
	}



	public CassandraConfig(String profileKeySpace, String profileColFamily,
			String pageRowSize, String profilePrimaryKey,
			String recommendationUpdateQuery, String recommendationKeySpace,
			String recommendationColFamily, String recommendationPrimaryKey) {
		
		this.profileKeySpace = profileKeySpace;
		this.profileColFamily = profileColFamily;
		this.pageRowSize = pageRowSize;
		this.profilePrimaryKey = profilePrimaryKey;
		this.recommendationUpdateQuery = recommendationUpdateQuery;
		this.recommendationKeySpace = recommendationKeySpace;
		this.recommendationColFamily = recommendationColFamily;
		this.recommendationPrimaryKey = recommendationPrimaryKey;	
	}

}
