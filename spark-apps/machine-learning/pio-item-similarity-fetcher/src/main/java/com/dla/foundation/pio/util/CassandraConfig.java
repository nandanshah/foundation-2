package com.dla.foundation.pio.util;

import java.io.Serializable;

public class CassandraConfig implements Serializable {
	public final String destinationUpdateQuery;
	public final String destinationKeySpace;
	public final String destinationColFamily;
	public final String destinationPrimaryKey;
	public final String sourceKeySpace;
	public final String sourceColFamily;
	public final String pageRowSize;
	public final String sourcePrimaryKey;

	public CassandraConfig() {
		this(null, null, null, null, null, null, null, null);
	}



	public CassandraConfig(String sourceKeySpace, String sourceColFamily,
			String pageRowSize, String sourcePrimaryKey,
			String destinationUpdateQuery, String destinationKeySpace,
			String destinationColFamily, String destinationPrimaryKey) {
		
		this.sourceKeySpace = sourceKeySpace;
		this.sourceColFamily = sourceColFamily;
		this.pageRowSize = pageRowSize;
		this.sourcePrimaryKey = sourcePrimaryKey;
		this.destinationUpdateQuery = destinationUpdateQuery;
		this.destinationKeySpace = destinationKeySpace;
		this.destinationColFamily = destinationColFamily;
		this.destinationPrimaryKey = destinationPrimaryKey;	
	}

}
