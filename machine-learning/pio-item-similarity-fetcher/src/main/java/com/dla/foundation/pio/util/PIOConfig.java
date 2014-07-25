package com.dla.foundation.pio.util;

import java.io.Serializable;

public class PIOConfig implements Serializable{
	private static final int DEFAULT_API_PORT_NUM = 8000;
	private static final int DEFAULT_NUM_REC = 8000;
	public final String appURL;
	public final String engineName;
	public final String appKey;
	public final int numRecPerItem;

	

	public PIOConfig() {
		this(null, null, null,DEFAULT_NUM_REC );
	}

	public PIOConfig(String appKey, String appURL, String engineName,
			int numRecPerItem) {
		this.appKey = appKey;
		this.appURL = appURL;
		this.engineName = engineName;
		this.numRecPerItem = numRecPerItem;

	}

}
