package com.dla.foundation.crawler.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.dla.foundation.analytics.utils.PropertiesHandler;

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
						String socialID = null, profileId = null;
						Map<String, ByteBuffer> keys = tuple._1();

						if (keys != null) {
							profileId = UUIDType.instance.compose(
									keys.get(profileIdKey)).toString();

							socialID = ByteBufferUtil.string(tuple._2
									.get(socialIdKey));

							return new Tuple2<String, String>(profileId,
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
		public final String fisKeyspace, analyticsKeyspace;
		public final String profileIdKey, socialIdKey;
		public final String profileCF, socialProfileCF, friendsCF;

		public CrawlerConfig() {
			this(null, null, null, null, null, null, null, null);
		}

		public CrawlerConfig(String lastCrawlerRunTimeKey, String fisKeyspace,
				String analyticsKeyspace, String profileIdKey,
				String socialIdKey, String profileCF, String socialProfileCF,
				String friendsCF) {
			super();
			this.lastCrawlerRunTimeKey = lastCrawlerRunTimeKey;
			this.fisKeyspace = fisKeyspace;
			this.analyticsKeyspace = analyticsKeyspace;
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
		String fisKeyspace = phandler.getValue(CrawlerPropKeys.fisKeyspace
				.getValue());
		String analyticsKeyspace = phandler
				.getValue(CrawlerPropKeys.analyticsKeyspace.getValue());
		String profileIdKey = phandler.getValue(CrawlerPropKeys.profileIdKey
				.getValue());
		String socialIdKey = phandler.getValue(CrawlerPropKeys.socialIdKey
				.getValue());
		String profileCF = phandler.getValue(CrawlerPropKeys.profilColumnFamily
				.getValue());
		String socialProfileCF = phandler
				.getValue(CrawlerPropKeys.socialProfileColumnFamily.getValue());
		String friendsCF = phandler
				.getValue(CrawlerPropKeys.friendsColumnFamily.getValue());
		String lastCrawlerRunTimeKey = phandler
				.getValue(CrawlerPropKeys.lastCrawlerRunTimeKey.getValue());

		return new CrawlerConfig(lastCrawlerRunTimeKey, fisKeyspace,
				analyticsKeyspace, profileIdKey, socialIdKey, profileCF,
				socialProfileCF, friendsCF);
	}

	public static GigyaConfig initGigyaConfig(PropertiesHandler phandler)
			throws IOException {
		String apiKey = phandler.getValue(CrawlerPropKeys.gigyaApiKey
				.getValue());
		String secretKey = phandler.getValue(CrawlerPropKeys.gigyaSecretKey
				.getValue());
		String apiScheme = phandler.getValue(CrawlerPropKeys.gigyaApiScheme
				.getValue());
		String apiDomain = phandler.getValue(CrawlerPropKeys.gigyaApiDomain
				.getValue());
		int timeoutMillis = Integer.parseInt(phandler
				.getValue(CrawlerPropKeys.gigyaTimeoutMillis.getValue()));
		return new GigyaConfig(apiKey, secretKey, apiScheme, apiDomain,
				timeoutMillis);
	}

	public static CassandraConfig initCassandraConfig(PropertiesHandler phandler)
			throws IOException {

		String[] cassandraIP = phandler.getValue(
				CrawlerPropKeys.cassandraIPList.getValue()).split(DEFAULT_SEP);
		String cassandraPort = phandler.getValue(CrawlerPropKeys.cassandraPort
				.getValue());
		String inputPartitioner = phandler
				.getValue(CrawlerPropKeys.inputPartitioner.getValue());
		String outputPartitioner = phandler
				.getValue(CrawlerPropKeys.outputPartitioner.getValue());
		String inputCQLPageRowSize = phandler
				.getValue(CrawlerPropKeys.inputCQLPageRowSize.getValue());

		return new CassandraConfig(cassandraIP, cassandraPort,
				inputPartitioner, outputPartitioner, inputCQLPageRowSize);
	}

}
