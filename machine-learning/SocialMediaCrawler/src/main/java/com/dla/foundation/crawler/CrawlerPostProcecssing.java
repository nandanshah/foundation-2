package com.dla.foundation.crawler;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.crawler.util.SparkCrawlerUtils;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CrawlerConfig;
import com.dla.foundation.model.FriendsInfo;
import com.dla.foundation.model.FriendsInfoResponse.Friend;
import com.dla.foundation.model.Profile;
import com.dla.foundation.model.UserProfileResponse;

/**
 * Class with methods required for Post Processing data after successful crawler
 * fetch
 */
public class CrawlerPostProcecssing implements Serializable {
	private static final long serialVersionUID = 8018799352410655286L;

	/**
	 * This function run post Processing for friends info crawled. It converts
	 * thte target site userid to DLA userid by running a join on profile data
	 * which contains DLA userid -> social site user id mapping with output from
	 * crawler which has social site user id for friends.
	 */
	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> runFriendsInfoPostProcessing(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraCon, CrawlerConfig crawlerConf,
			JavaPairRDD<Long, Friend> inputfriendsInfo,
			String inputCQLPageRowSize, String relation) {

		JavaPairRDD<Tuple2<Long, String>, String> friendsInfo = extractFriendsInfo(
				inputfriendsInfo, relation);
		return runPostProcessingInternal(sparkContext, cassandraCon,
				crawlerConf, friendsInfo, inputCQLPageRowSize);
	}

	/**
	 * Method to convert input of type <K,V> <dlaid,Friend> to <K,V>
	 * <Tuple<dlaid,socialauth>, Relation>
	 * 
	 * @param inputfriendsInfo
	 * @param relation
	 * @return
	 */
	private static JavaPairRDD<Tuple2<Long, String>, String> extractFriendsInfo(
			JavaPairRDD<Long, Friend> inputfriendsInfo, final String relation) {

		JavaPairRDD<Tuple2<Long, String>, String> transformedTuple = inputfriendsInfo
				.mapToPair(new PairFunction<Tuple2<Long, Friend>, Tuple2<Long, String>, String>() {
					private static final long serialVersionUID = -7922794218894809033L;

					@Override
					public Tuple2<Tuple2<Long, String>, String> call(
							Tuple2<Long, Friend> tuple) throws Exception {
						Long profileId = tuple._1;
						Friend friend = tuple._2;
						String friendUserId = friend.UID;
						Tuple2<Long, String> userids = new Tuple2<Long, String>(
								profileId, friendUserId);
						return new Tuple2<Tuple2<Long, String>, String>(
								userids, relation);
					}
				});
		return transformedTuple;
	}

	private static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> runPostProcessingInternal(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraCon, CrawlerConfig crawlerConf,
			JavaPairRDD<Tuple2<Long, String>, String> friendsInfo,
			String inputCQLPageRowSize) {

		Configuration conf = new Configuration();
		// Reading profile table from Cassandra
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> profileRDD = cassandraCon
				.read(conf, sparkContext, crawlerConf.keySpace,
						crawlerConf.profileCF, inputCQLPageRowSize);

		// Getting dla and social id from profile columnfamily
		JavaPairRDD<String, Long> userSocialPair = SparkCrawlerUtils
				.getSocialIDLAandID(profileRDD, crawlerConf.profileIdKey,
						crawlerConf.socialIdKey);

		// Transforming data to have social auth as a key and rest as value
		JavaPairRDD<String, Tuple2<Long, String>> transformedFriendsInfo = transformFriendsInfo(friendsInfo);

		// Joining dataset on social auth(key) to get dlaid for friend
		// socialauth
		JavaPairRDD<String, Tuple2<Long, Tuple2<Long, String>>> joinedSet = userSocialPair
				.join(transformedFriendsInfo);

		// Format data in the Cassandra format
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraFormattedFrdInfo = formatDataForCassandra(
				joinedSet, crawlerConf.frdsIdKeySep);

		// Removing null records
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> filteredFrdsInfo = SparkCrawlerUtils
				.filter(cassandraFormattedFrdInfo);

		return filteredFrdsInfo;
	}

	/**
	 * Method to convert input of type <K,V> <Tuple<dlaid,socialauth>, Relation>
	 * to <K,V> <socialauth, Tuple<dlaid,relation>>
	 * 
	 * @param friendsInfo
	 * @return
	 */
	private static JavaPairRDD<String, Tuple2<Long, String>> transformFriendsInfo(
			JavaPairRDD<Tuple2<Long, String>, String> friendsInfo) {
		JavaPairRDD<String, Tuple2<Long, String>> transformedFrdsInfo = friendsInfo
				.mapToPair(new PairFunction<Tuple2<Tuple2<Long, String>, String>, String, Tuple2<Long, String>>() {

					private static final long serialVersionUID = -7922794218894809033L;

					@Override
					public Tuple2<String, Tuple2<Long, String>> call(
							Tuple2<Tuple2<Long, String>, String> tuple)
							throws Exception {
						Tuple2<Long, String> inTuple = tuple._1;
						Long userid = inTuple._1;
						String socialAuth = inTuple._2;
						String relation = tuple._2;
						Tuple2<Long, String> valTuple = new Tuple2<Long, String>(
								userid, relation);
						Tuple2<String, Tuple2<Long, String>> outtuple = new Tuple2<String, Tuple2<Long, String>>(
								socialAuth, valTuple);
						return outtuple;
					}
				});
		return transformedFrdsInfo;
	}

	/**
	 * Converts input tuple of format <K,V> <socialauth, tuple<frdUserId, tuple<
	 * dlauserid, relation>>> to cassandra format <K,V> <Map of keys, List of
	 * values>
	 * 
	 * @param joinedSet
	 * @param seperator
	 * @return
	 */
	private static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> formatDataForCassandra(
			JavaPairRDD<String, Tuple2<Long, Tuple2<Long, String>>> joinedSet,
			final String seperator) {

		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraformatFrdlist = joinedSet
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Tuple2<Long, String>>>, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					private static final long serialVersionUID = -7922794218894809033L;
					Map<String, ByteBuffer> keys;
					List<ByteBuffer> list;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<String, Tuple2<Long, Tuple2<Long, String>>> tuple)
							throws Exception {

						keys = new LinkedHashMap<String, ByteBuffer>();
						list = new ArrayList<ByteBuffer>();

						Tuple2<Long, Tuple2<Long, String>> value = tuple._2;

						Long frdUserId = value._1;
						Tuple2<Long, String> userFrdTup = value._2;
						Long userid = userFrdTup._1;
						String relation = userFrdTup._2;

						String key = userid + seperator + frdUserId;

						keys.put(FriendsInfo.userfrdpair.getValue(),
								ByteBufferUtil.bytes(key));

						list.add(ByteBufferUtil.bytes(relation));

						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								keys, list);
					}
				});
		return cassandraformatFrdlist;
	}

	/**
	 * Converts input tuple of format <K,V> <dlaid, UserProfileResponse> to
	 * cassandra format <K,V> <Map of keys, List of values> by extracting
	 * required features from response. Currently, have extracted required
	 * fields here will make it more dynamic
	 * 
	 * @param socialProfile
	 * @return
	 */
	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> formatSocialProfileForCassandra(
			JavaPairRDD<Long, UserProfileResponse> socialProfile) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> formattedSocialProfile = socialProfile
				.mapToPair(new PairFunction<Tuple2<Long, UserProfileResponse>, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					private static final long serialVersionUID = 1032888592844095856L;
					Map<String, ByteBuffer> keys;
					List<ByteBuffer> list;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<Long, UserProfileResponse> tuple)
							throws Exception {

						keys = new LinkedHashMap<String, ByteBuffer>();
						list = new ArrayList<ByteBuffer>();

						keys.put(Profile.id.getValue(),
								ByteBufferUtil.bytes(tuple._1()));

						UserProfileResponse response = tuple._2;

						// Improvise on Schema (Evolving)
						list.add(ByteBufferUtil.bytes(response.firstName
								+ response.lastName));
						list.add(ByteBufferUtil.bytes(response.nickname));
						String locale = response.locale != null ? response.locale
								: "NA";
						list.add(ByteBufferUtil.bytes(locale));
						String gender = response.gender != null ? response.gender
								: "NA";
						list.add(ByteBufferUtil.bytes(gender));
						String country = response.country; 
						list.add(ByteBufferUtil.bytes(country != null? country : "NA" ));
						String timezone = String.valueOf(response.timezone);
						list.add(ByteBufferUtil.bytes(timezone!= null? timezone : "NA" ));

						return new Tuple2<Map<String, ByteBuffer>, java.util.List<ByteBuffer>>(
								keys, list);
					}
				});

		return formattedSocialProfile;
	}

	/**
	 * Converts input tuple of format <K,V> <dlaid, UserProfileResponse> to
	 * cassandra format <K,V> <Map of keys, List of values> updating
	 * lastmodified field to new value after successful profile fetch
	 * 
	 * @param inputRDD
	 * @param newLastModified
	 * @return
	 */
	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> prepareForProfileUpdate(
			JavaPairRDD<Long, UserProfileResponse> inputRDD,
			final long newLastModified) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraFormattedRDD = inputRDD
				.mapToPair(new PairFunction<Tuple2<Long, UserProfileResponse>, Map<String, ByteBuffer>, List<ByteBuffer>>() {
					private static final long serialVersionUID = -3277073504429627935L;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<Long, UserProfileResponse> tuple)
							throws Exception {

						Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
						List<ByteBuffer> list = new ArrayList<ByteBuffer>();

						keys.put(Profile.id.getValue(),
								ByteBufferUtil.bytes(tuple._1()));

						list.add(ByteBufferUtil.bytes(newLastModified));
						Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> retTuple = new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								keys, list);

						return retTuple;
					}
				});
		return cassandraFormattedRDD;
	}
}
