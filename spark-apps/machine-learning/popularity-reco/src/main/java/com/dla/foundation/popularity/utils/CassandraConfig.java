package com.dla.foundation.popularity.utils;

import java.io.Serializable;

public class CassandraConfig implements Serializable {
	public final String platformKeySpace;
	public final String analyticsKeySpace;
	public final String pageRowSize; 
	
	public CassandraConfig(String platformKeySpace, String analyticsKeySpace, String pageRowSize) {
		this.platformKeySpace = platformKeySpace;
		this.analyticsKeySpace = analyticsKeySpace;
		this.pageRowSize = pageRowSize;
	}
}
