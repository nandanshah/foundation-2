package com.dla.foundation.crawler.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.model.Profile;
import scala.Tuple2;

public class SparkCrawlerUtils {

	public static final String DEFAULT_SEP = ",";

	/**
	 * Utility function to filter out records having null input tuples. This is
	 * required in cases where gigya call fails (in which case it returns null
	 * tuple)
	 * 
	 * @param inputRDD
	 * @return filtered RDD
	 */
	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> filter(
			JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> inputRDD) {

		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> outputRDD = inputRDD
				.filter(new Function<Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>, Boolean>() {

					private static final long serialVersionUID = -1406718619849210965L;

					@Override
					public Boolean call(
							Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> tuple)
							throws Exception {
						if (tuple != null) {
							if (tuple._1 != null && tuple._2 != null)
								return true;
						}
						return false;
					}
				});

		return outputRDD;
	}

	/**
	 * Utility to get id and social id from input data It extracts dlaid
	 * 'profileIdKey' and socialid 'socialIdKey' from input Cassandra RDD format
	 * having Map of keys and Values In case of profile table profileidKey will
	 * be in keys map as <id, 1234> and socialauth will be in values map as
	 * <socialauth, "_sdfsdf-sdfasdf_wxags=">
	 * 
	 * @param cassandraRDD
	 * @param profileIdKey
	 * @param socialIdKey
	 * @param timeStampKey
	 * @return
	 */
	public static JavaPairRDD<String, String> getIDLAandSocialID(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,
			final String profileIdKey, final String socialIdKey) {

		JavaPairRDD<String, String> pairs = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> tuple)
							throws Exception {
						String socialID = null, profileId = null, accountId = null, primaryKey = null;
						Map<String, ByteBuffer> keys = tuple._1();

						if (keys != null) {
							for (Entry<String, ByteBuffer> column : keys
									.entrySet()) {

								if (column.getKey().toLowerCase()
										.compareTo(Profile.id.getValue()) == 0) {
									if (null != column.getValue())
										profileId = UUIDType.instance.compose(column
												.getValue()).toString();
								}else if(column.getKey().toLowerCase()
										.compareTo(Profile.accountid.getValue()) == 0) {
									if (null != column.getValue())
										accountId = UUIDType.instance.compose(column
												.getValue()).toString();
							}
						}
							
						socialID = ByteBufferUtil.string(tuple._2
									.get(socialIdKey));
						primaryKey = profileId+"#"+accountId;
						
						return new Tuple2<String, String>(primaryKey,
									socialID);
						} else {
							return new Tuple2<String, String>(null, null);
						}
					}
				
				});
		return pairs;
	}

	/**
	 * Utility to get id and social id from input data It extracts dlaid
	 * 'profileIdKey' and socialid 'socialIdKey' from input Cassandra RDD format
	 * having Map of keys and Values In case of profile table profileidKey will
	 * be in keys map as <id, 1234> and socialauth will be in values map as
	 * <socialauth, "_sdfsdf-sdfasdf_wxags="> Only difference to above function
	 * is it returns socialId as key and profileId as valaue
	 * 
	 * @param cassandraRDD
	 * @param profileIdKey
	 * @param socialIdKey
	 * @param timeStampKey
	 * @return
	 */
	public static JavaPairRDD<String, String> getSocialIDLAandID(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,
			final String profileIdKey, final String socialIdKey) {

		JavaPairRDD<String, String> pairs = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> tuple)
							throws Exception {
						String socialID = null;
						String profileId = null;
						Map<String, ByteBuffer> keys = tuple._1();

						if (keys != null) {
							ByteBuffer buff = keys.get(profileIdKey);
							if (buff != null)
								profileId = UUIDType.instance.compose(buff)
										.toString();

							socialID = ByteBufferUtil.string(tuple._2
									.get(socialIdKey));

							return new Tuple2<String, String>(socialID,
									profileId);
						} else {
							return new Tuple2<String, String>(null, null);
						}
					}
				});
		return pairs;
	}

	// Classes to hold property values together

	/**
	 * Hold Crawler specific properties, which may change for each run
	 * 
	 */
	public static class CrawlerConfig {
		public final String lastCrawlerRunTimeKey;
		public final String fisKeyspace, platformKeyspace;
		public final String profileIdKey, socialIdKey;
		public final String profileCF, socialProfileCF, friendsCF;

		public CrawlerConfig() {
			this(null, null, null, null, null, null, null, null);
		}

		public CrawlerConfig(String lastCrawlerRunTimeKey, String fisKeyspace,
				String platformKeyspace, String profileIdKey,
				String socialIdKey, String profileCF, String socialProfileCF,
				String friendsCF) {
			super();
			this.lastCrawlerRunTimeKey = lastCrawlerRunTimeKey;
			this.fisKeyspace = fisKeyspace;
			this.platformKeyspace = platformKeyspace;
			this.profileIdKey = profileIdKey;
			this.socialIdKey = socialIdKey;
			this.profileCF = profileCF;
			this.socialProfileCF = socialProfileCF;
			this.friendsCF = friendsCF;
		}

	}

	/**
	 * Wrapper to hold Cassandra properties
	 */
	public static class CassandraConfig {
		public final String[] ipList;
		public final String port;
		public final String inputPartitioner, outputPartitioner;
		public final String inputCQLPageRowSize;

		public CassandraConfig() {
			this(null, null, null, null, null);
		}

		public CassandraConfig(String[] cassandraIP, String cassandraPort,
				String inputPartitioner, String outputPartitioner,
				String inputCQLPageRowSize) {
			this.ipList = cassandraIP;
			this.port = cassandraPort;
			this.inputPartitioner = inputPartitioner;
			this.outputPartitioner = outputPartitioner;
			this.inputCQLPageRowSize = inputCQLPageRowSize;
		}

	}

	/**
	 * Wrapper to hold Gigya properties
	 */
	public static class GigyaConfig {
		public final String apiKey;
		public final String secretKey;
		public final String apiScheme;
		public final String apiDomain;
		public final int timeoutMillis;

		public GigyaConfig() {
			this(null, null, null, null, 0);
		}

		public GigyaConfig(String apiKey, String secretKey, String apiScheme,
				String apiDomain, int timeoutMillis) {
			this.apiKey = apiKey;
			this.secretKey = secretKey;
			this.apiScheme = apiScheme;
			this.apiDomain = apiDomain;
			this.timeoutMillis = timeoutMillis;
		}

	}

	// Init methods to load properties from Config file

	public static CrawlerConfig initCrawlerConfig(PropertiesHandler phandler)
			throws IOException {
		String fisKeyspace = phandler.getValue(CommonPropKeys.cs_fisKeyspace
				.getValue());
		String platformKeyspace = phandler
				.getValue(CommonPropKeys.cs_platformKeyspace.getValue());
		String profileIdKey = CrawlerStaticPropKeys.PROFILE_ID_KEY;
		String socialIdKey = CrawlerStaticPropKeys.SOCIAL_ID_KEY;
		String profileCF = CrawlerStaticPropKeys.PROFILE_INP_CF;
		String socialProfileCF = CrawlerStaticPropKeys.SOCIALPROFILE_OUT_CF;
		String friendsCF = CrawlerStaticPropKeys.FRIENDSINFO_OUT_CF;
		String lastCrawlerRunTimeKey = CrawlerStaticPropKeys.LAST_CRAWLER_RUN_TIME_KEY;

		return new CrawlerConfig(lastCrawlerRunTimeKey, fisKeyspace,
				platformKeyspace, profileIdKey, socialIdKey, profileCF,
				socialProfileCF, friendsCF);
	}

	public static GigyaConfig initGigyaConfig(PropertiesHandler phandler)
			throws IOException {
		String apiKey = phandler.getValue(CommonPropKeys.gigya_ApiKey
				.getValue());
		String secretKey = phandler.getValue(CommonPropKeys.gigya_SecretKey
				.getValue());
		String apiScheme = phandler.getValue(CommonPropKeys.gigya_ApiScheme
				.getValue());
		String apiDomain = phandler.getValue(CommonPropKeys.gigya_ApiDomain
				.getValue());
		int timeoutMillis = Integer.parseInt(phandler
				.getValue(CommonPropKeys.gigya_TimeoutMillis.getValue()));
		return new GigyaConfig(apiKey, secretKey, apiScheme, apiDomain,
				timeoutMillis);
	}

	public static CassandraConfig initCassandraConfig(PropertiesHandler phandler)
			throws IOException {

		String[] cassandraIP = phandler.getValue(
				CommonPropKeys.cs_hostList.getValue()).split(DEFAULT_SEP);
		String cassandraPort = phandler.getValue(CommonPropKeys.cs_rpcPort
				.getValue());
		String inputPartitioner = CrawlerStaticPropKeys.PARTITIONER;
		String outputPartitioner = CrawlerStaticPropKeys.PARTITIONER;
		String inputCQLPageRowSize = phandler
				.getValue(CommonPropKeys.cs_pageRowSize.getValue());

		return new CassandraConfig(cassandraIP, cassandraPort,
				inputPartitioner, outputPartitioner, inputCQLPageRowSize);
	}

}
