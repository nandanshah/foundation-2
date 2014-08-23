package com.dla.foundation.pio.util;

import java.io.Serializable;

public class CassandraConfig implements Serializable {
	public final String fisKeySpace;
	public final String pageRowSize;
	public final String platformKeySpace;
	public final String recommendationColFamily;
	public final String profileColFamily;
	public final String accountColFamily;

	
	public CassandraConfig(String platformKeySpace, String fisKeySpace,
			String profileColFamily, String accountColFamily,
			String recommendationColFamily, String pageRowSize) {

		this.platformKeySpace = platformKeySpace;
		this.fisKeySpace = fisKeySpace;
		this.profileColFamily = profileColFamily;
		this.recommendationColFamily = recommendationColFamily;
		this.accountColFamily = accountColFamily;
		this.pageRowSize = pageRowSize;

	}

}
