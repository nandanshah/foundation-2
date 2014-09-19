package com.dla.foundation.pio.util;

import java.io.Serializable;

public class RecoFetcherConstants implements Serializable{
	public static final double RECO_SCORE = 1;
	public static final String RECO_REASON = "By Machine Learning";
	public static final int EVENT_REQ_FLAG = 1;
	public static final String DEFAULT_API_PORT_NUM = "8000";
	public static final String APPNAME = "pioRecoFetcher";
	public static final String RECOMMEND_CF= "pio_user_reco";
	public static final String PIO_NUM_REC_PER_USER= "numRecsPerUser";
	public static final String INPUT_PARTITIONER = "Murmur3Partitioner";
	public static final String OUTPUT_PARTITIONER = "Murmur3Partitioner";
	

}
