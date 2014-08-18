package com.dla.foundation.crawler;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.crawler.util.SparkCrawlerUtils;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CrawlerConfig;
import com.dla.foundation.model.FriendsInfo;
import com.dla.foundation.model.FriendsInfoResponse.Friend;
import com.dla.foundation.model.Profile;
import com.dla.foundation.model.UserProfileResponse;
import com.dla.foundation.analytics.utils.CassandraSparkConnector;

/**
 * Class with methods required for Post Processing data after successful crawler
 * fetch
 */
public class CrawlerPostProcecssing implements Serializable {
	private static final long serialVersionUID = 8018799352410655286L;
	private static final String NOT_AVALAIBLE = "NA";

	/**
	 * This function run post Processing for friends info crawled. It converts
	 * thte target site userid to DLA userid by running a join on profile data
	 * which contains DLA userid -> social site user id mapping with output from
	 * crawler which has social site user id for friends.
	 */
	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> runFriendsInfoPostProcessing(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraCon, CrawlerConfig crawlerConf,
			JavaPairRDD<String, Friend> inputfriendsInfo,
			String inputCQLPageRowSize, String relation) {

		JavaPairRDD<Tuple2<String, String>, String> friendsInfo = extractFriendsInfo(
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
	private static JavaPairRDD<Tuple2<String, String>, String> extractFriendsInfo(
			JavaPairRDD<String, Friend> inputfriendsInfo, final String relation) {

		JavaPairRDD<Tuple2<String, String>, String> transformedTuple = inputfriendsInfo
				.mapToPair(new PairFunction<Tuple2<String, Friend>, Tuple2<String, String>, String>() {
					private static final long serialVersionUID = -7922794218894809033L;

					@Override
					public Tuple2<Tuple2<String, String>, String> call(
							Tuple2<String, Friend> tuple) throws Exception {
						String profileId = tuple._1;
						Friend friend = tuple._2;
						String friendUserId = friend.UID;
						Tuple2<String, String> userids = new Tuple2<String, String>(
								profileId, friendUserId);
						return new Tuple2<Tuple2<String, String>, String>(
								userids, relation);
					}
				});
		return transformedTuple;
	}

	private static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> runPostProcessingInternal(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraCon, CrawlerConfig crawlerConf,
			JavaPairRDD<Tuple2<String, String>, String> friendsInfo,
			String inputCQLPageRowSize) {

		Configuration conf = new Configuration();
		// Reading profile table from Cassandra
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> profileRDD = cassandraCon
				.read(conf, sparkContext, crawlerConf.analyticsKeyspace,
						crawlerConf.profileCF, inputCQLPageRowSize);

		// Getting dla and social id from profile columnfamily
		JavaPairRDD<String, String> userSocialPair = SparkCrawlerUtils
				.getSocialIDLAandID(profileRDD, crawlerConf.profileIdKey,
						crawlerConf.socialIdKey);

		// Transforming data to have social auth as a key and rest as value
		JavaPairRDD<String, Tuple2<String, String>> transformedFriendsInfo = transformFriendsInfo(friendsInfo);

		// Joining dataset on social auth(key) to get dlaid for friend
		// socialauth
		JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> joinedSet = userSocialPair
				.join(transformedFriendsInfo);

		// Format data in the Cassandra format
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraFormattedFrdInfo = formatDataForCassandra(joinedSet);

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
	private static JavaPairRDD<String, Tuple2<String, String>> transformFriendsInfo(
			JavaPairRDD<Tuple2<String, String>, String> friendsInfo) {
		JavaPairRDD<String, Tuple2<String, String>> transformedFrdsInfo = friendsInfo
				.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, String>, String, Tuple2<String, String>>() {

					private static final long serialVersionUID = -7922794218894809033L;

					@Override
					public Tuple2<String, Tuple2<String, String>> call(
							Tuple2<Tuple2<String, String>, String> tuple)
							throws Exception {
						Tuple2<String, String> inTuple = tuple._1;
						String userid = inTuple._1;
						String socialAuth = inTuple._2;
						String relation = tuple._2;
						Tuple2<String, String> valTuple = new Tuple2<String, String>(
								userid, relation);
						Tuple2<String, Tuple2<String, String>> outtuple = new Tuple2<String, Tuple2<String, String>>(
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
			JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> joinedSet) {

		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraformatFrdlist = joinedSet
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Tuple2<String, String>>>, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					private static final long serialVersionUID = -7922794218894809033L;
					Map<String, ByteBuffer> keys;
					List<ByteBuffer> list;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<String, Tuple2<String, Tuple2<String, String>>> tuple)
							throws Exception {

						keys = new LinkedHashMap<String, ByteBuffer>();
						list = new ArrayList<ByteBuffer>();

						Tuple2<String, Tuple2<String, String>> value = tuple._2;

						String frdUserId = value._1;
						Tuple2<String, String> userFrdTup = value._2;
						String userid = userFrdTup._1;
						String relation = userFrdTup._2;

						keys.put(FriendsInfo.profileid.getValue(),
								UUIDType.instance.fromString(userid));
						keys.put(FriendsInfo.friendid.getValue(),
								UUIDType.instance.fromString(frdUserId));

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
			JavaPairRDD<String, UserProfileResponse> socialProfile) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> formattedSocialProfile = socialProfile
				.mapToPair(new PairFunction<Tuple2<String, UserProfileResponse>, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					private static final long serialVersionUID = 1032888592844095856L;
					Map<String, ByteBuffer> keys;
					List<ByteBuffer> list;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<String, UserProfileResponse> tuple)
							throws Exception {

						if (tuple == null)
							return null;

						keys = new LinkedHashMap<String, ByteBuffer>();
						list = new ArrayList<ByteBuffer>();

						keys.put(Profile.id.getValue(),
								UUIDType.instance.fromString(tuple._1()));

						UserProfileResponse response = tuple._2;
						// Improvise on Schema (Evolving)

						String username = ((response.firstName != null && response.lastName != null)) ? (response.firstName
								+ " " + response.lastName)
								: NOT_AVALAIBLE;
						list.add(ByteBufferUtil.bytes(username));
						String nickname = (response.nickname != null && !response.nickname
								.equalsIgnoreCase("")) ? response.nickname
								: NOT_AVALAIBLE;
						list.add(ByteBufferUtil.bytes(nickname));
						String locale = response.locale != null ? response.locale
								: NOT_AVALAIBLE;
						list.add(ByteBufferUtil.bytes(locale));
						String gender = response.gender != null ? response.gender
								: NOT_AVALAIBLE;
						list.add(ByteBufferUtil.bytes(gender));
						String country = response.country;
						list.add(ByteBufferUtil.bytes(country != null ? country
								: NOT_AVALAIBLE));
						String timezone = String.valueOf(response.timezone);
						list.add(ByteBufferUtil
								.bytes(timezone != null ? timezone
										: NOT_AVALAIBLE));

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
			JavaPairRDD<String, UserProfileResponse> inputRDD,
			final long newLastModified) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraFormattedRDD = inputRDD
				.mapToPair(new PairFunction<Tuple2<String, UserProfileResponse>, Map<String, ByteBuffer>, List<ByteBuffer>>() {
					private static final long serialVersionUID = -3277073504429627935L;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<String, UserProfileResponse> tuple)
							throws Exception {

						if (tuple == null)
							return null;

						Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
						List<ByteBuffer> list = new ArrayList<ByteBuffer>();

						keys.put(Profile.id.getValue(),
								UUIDType.instance.fromString(tuple._1()));

						list.add(ByteBufferUtil.bytes(newLastModified));
						Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> retTuple = new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								keys, list);

						return retTuple;
					}
				});
		return cassandraFormattedRDD;
	}

	public static JavaPairRDD<String, UserProfileResponse> filterSocialProfile(
			JavaPairRDD<String, UserProfileResponse> rawsocialProfile) {

		JavaPairRDD<String, UserProfileResponse> filteredSocialProfile = rawsocialProfile
				.filter(new Function<Tuple2<String, UserProfileResponse>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(
							Tuple2<String, UserProfileResponse> tuple)
							throws Exception {
						if (tuple != null && tuple._2 != null)
							return true;
						return false;
					}
				});

		return filteredSocialProfile;
	}
}
