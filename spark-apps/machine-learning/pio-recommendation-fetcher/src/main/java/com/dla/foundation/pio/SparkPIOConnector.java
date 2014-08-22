package com.dla.foundation.pio;

import io.prediction.Client;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.pio.util.ColumnCollection;
import com.dla.foundation.pio.util.RecoFetcherConstants;
import com.dla.foundation.pio.util.UserProfile;

public class SparkPIOConnector implements Serializable {

	private static final long serialVersionUID = 8073585091774712586L;
	private static final String VALUE_DELIMETER = "#";
	protected static final Integer USER_COUNT_INCR = 1;
	private static Logger logger = Logger.getLogger(SparkPIOConnector.class
			.getName());

	/**
	 * Instantiates and return JavaSparkContext.
	 * 
	 * @param sparkMaster
	 *            Name of Master for JavaSparkContext.
	 * @param sparkAppName
	 *            Name of AppName for JavaSparkContext.
	 * @return Instance of JavaSparkContext.
	 */
	public JavaSparkContext initilizeSparkContext(String sparkMaster,
			String sparkAppName) {

		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster(sparkMaster);
		sparkConf.setAppName(sparkAppName);

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		return sparkContext;

	}

	/**
	 * Get ALL recommendations for provided list of UserProfile from PIO with
	 * SparkContext. (This method will return empty list of recommendation for
	 * users - PIO not able to return results.)
	 * 
	 * @param userProfileRDD
	 *            RDD of UserProfile for whom recommendations to be fetched.
	 * @param strAppKey
	 *            AppKey that Client will use to communicate with API.
	 * @param strAppURL
	 *            AppURL that Client will use to communicate with API
	 * @param strEngine
	 *            EngineName that Client will use to get Recommendations.
	 * @param intNumRecPerUser
	 *            Maximum number of recommendations to be fetch per user.
	 * @param accumRecoFailedUsers
	 *            : a accumulator variable to count number of users for which
	 *            PIO not able to return any recommendations.
	 * @param accumAllUsers
	 *            : a accumulator variable to count number of users for which we
	 *            are going to fetch reco from PIO.
	 * 
	 * @return JavaPairRDD<String, List<String>> Key-Value pair where Key is
	 *         UserProfile and value is List of Recommendations.
	 */

	public JavaPairRDD<UserProfile, List<String>> getRecommendations(
			JavaRDD<UserProfile> userProfileRDD, final String strAppKey,
			final String strAppURL, final String strEngine,
			final int intNumRecPerUser,
			final Accumulator<Integer> accumAllUsers,
			final Accumulator<Integer> accumRecoFailedUsers) {
		logger.info("Started fetching recommendations for users from Prediction IO.");
		final JavaPairRDD<UserProfile, List<String>> recommendationsRDD = userProfileRDD
				.mapToPair(new PairFunction<UserProfile, UserProfile, List<String>>() {

					private static final long serialVersionUID = 5528816445669906910L;

					@SuppressWarnings("finally")
					@Override
					public Tuple2<UserProfile, List<String>> call(
							UserProfile user) throws Exception {
						String recommendationArray[] = {};
						Client client = null;
						try {
							// Currently PIO Client SDK is instantiated inside
							// call method because Client class does not
							// implements the Serilizable interface.
							client = new Client(strAppKey, strAppURL);
							client.identify(user.userID);
							accumAllUsers.add(USER_COUNT_INCR);
							logger.info("Fetching recommendations for user id "
									+ user.userID);
							// gets intNumRecPerUser recommendations from PIO.
							recommendationArray = client.getItemRecTopN(
									strEngine, intNumRecPerUser);

						} catch (IOException e) {
							logger.warn(e.getMessage());
							// If PIO doesn't find recommendation for particular
							// user it throws IOException, in such cases we are
							// returning empty array for same user.
							accumRecoFailedUsers.add(USER_COUNT_INCR);

						} finally {
							client.close();
							return new Tuple2<UserProfile, List<String>>(user,
									Arrays.asList(recommendationArray));
						}

					}

				});
		return recommendationsRDD;

	}

	/**
	 * This method will filter out empty recommendations and Returns list of
	 * users that have recommendations. If PIO doesn't find recommendation for
	 * particular user it throws IOException, in such cases we are returning
	 * empty array for same user. In this method we are removing those users for
	 * whom we have provided empty list of recommendations.
	 * 
	 * 
	 * @param allRecommendations
	 *            All recommendations fetched from PIO, including empty list of
	 *            recommendations.
	 * @return JavaPairRDD<String, List<String>> Key-Value pair where Key is
	 *         UserID and value is List of non-empty Recommendations.
	 */

	public JavaPairRDD<UserProfile, List<String>> removeEmptyRecommendations(
			JavaPairRDD<UserProfile, List<String>> allRecommendations) {
		JavaPairRDD<UserProfile, List<String>> filteredRecommendations = allRecommendations
				.filter(new Function<Tuple2<UserProfile, List<String>>, Boolean>() {

					private static final long serialVersionUID = -7711009473196986893L;

					@Override
					public Boolean call(
							Tuple2<UserProfile, List<String>> recommendation)
							throws Exception {
						// this method will return only users and those have
						// recommendation returned from PIO and their
						// recommendations.
						return !recommendation._2().isEmpty();
					}
				});
		return filteredRecommendations;
	}

	/**
	 * Returns JavaPairRDD in (<Map<String,
	 * ByteBuffer>,java.util.List<ByteBuffer>>) format that which is used for
	 * writing data to Cassandra.
	 * 
	 * @param userRecommendationsRDD
	 *            JavaPairRDD of users those have non-empty recommendation list.
	 * 
	 * 
	 * @return JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>>
	 *         JavaPairRDD in special format that is suitable for writing data
	 *         to Cassandra.
	 */

	public JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> formatRecommendationsForCassandraWrite(
			JavaPairRDD<UserProfile, String> userPerRecoRDD, final Date date) {

		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraRDD = userPerRecoRDD
				.mapToPair(new PairFunction<Tuple2<UserProfile, String>, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					private static final long serialVersionUID = 8894826755888085258L;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<UserProfile, String> recommendation)
							throws Exception {
						Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();

						List<ByteBuffer> values = new ArrayList<ByteBuffer>();

						UserProfile userProfile = recommendation._1();
						String itemid = recommendation._2();
						keys.put(ColumnCollection.PROFILE_ID, UUIDType.instance
								.fromString(userProfile.userID));
						keys.put(ColumnCollection.ITEM_ID,
								UUIDType.instance.fromString(itemid));
						logger.info("Started converting recommendation data to ByteBuffer format for profileid  "+keys.get(ColumnCollection.PROFILE_ID) +" and itemid "+keys.get(ColumnCollection.ITEM_ID));
						values.add(UUIDType.instance
								.fromString(userProfile.tenantID));
						values.add(UUIDType.instance
								.fromString(userProfile.regionID));
						values.add(TimestampType.instance.decompose(date));
						values.add(ByteBufferUtil
								.bytes(RecoFetcherConstants.RECO_SCORE));
						values.add(ByteBufferUtil
								.bytes(RecoFetcherConstants.RECO_REASON));
						values.add(ByteBufferUtil
								.bytes(RecoFetcherConstants.EVENT_REQ_FLAG));
						logger.info("Completed converting recommendation data to ByteBuffer format for profileid  "+keys.get(ColumnCollection.PROFILE_ID) +" and itemid "+keys.get(ColumnCollection.ITEM_ID));
						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								keys, values);
					}

				});

		return cassandraRDD;
	}

	/**
	 * 
	 * This method converts records read from Profile Table to JavaPairRDD,
	 * where key is accountID and value is a delimited string as :
	 * profileID#regionID.
	 * 
	 * @param profileCassandraRDD
	 *            : a JavaPairRDD<Map<String, ByteBuffer>, Map<String,
	 *            ByteBuffer>> where key represents map of all keys read from
	 *            cassandra CF and value represents a map of all values read
	 *            from cassandra CF Profile.
	 * @return : JavaPairRDD<String, String> where key represents accountID and
	 *         value is delimited String which holds profileID and regionID
	 */
	public JavaPairRDD<String, String> toProfilePairRDD(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> profileCassandraRDD) {
		return profileCassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {
						
					@Override
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> dataFromCassandra)
							throws Exception {
						Map<String, ByteBuffer> keySet = dataFromCassandra._1();
						ByteBuffer ID = keySet.get(ColumnCollection.ID);
						ByteBuffer accountID = keySet
								.get(ColumnCollection.ACCOUNT_ID);
						Map<String, ByteBuffer> valueMap = dataFromCassandra
								._2();

						ByteBuffer regionID = valueMap
								.get(ColumnCollection.HOME_REGION_ID);

						String newValue = UUIDType.instance
								.compose(ID)
								.toString()
								.concat(VALUE_DELIMETER)
								.concat(UUIDType.instance.compose(regionID)
										.toString());
						return new Tuple2<String, String>(UUIDType.instance
								.compose(accountID).toString(), newValue);

					}
				});
	}

	/**
	 * This method converts records read from Account Table to JavaPairRDD,
	 * where key is accountID and value is tenantID.
	 * 
	 * @param accountCassandraRDD
	 *            :a JavaPairRDD<Map<String, ByteBuffer>, Map<String,
	 *            ByteBuffer>> where key represents map of all keys read from
	 *            cassandra CF and value represents a map of all values read
	 *            from cassandra CF Account
	 * @return JavaPairRDD<String, String> where key represents accountID and
	 *         value is TenantID.
	 */
	public JavaPairRDD<String, String> toAccountPairRDD(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> accountCassandraRDD) {
		return accountCassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					@Override
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> dataFromCassandra)
							throws Exception {
						Map<String, ByteBuffer> keySet = dataFromCassandra._1();
						ByteBuffer accountID = keySet.get(ColumnCollection.ID);
						Map<String, ByteBuffer> valueMap = dataFromCassandra
								._2();
						ByteBuffer tenantID = valueMap
								.get(ColumnCollection.TENANT_ID);

						return new Tuple2<String, String>(UUIDType.instance
								.compose(accountID).toString(),
								UUIDType.instance.compose(tenantID).toString());

					}
				});
	}

	/**
	 * This method will return account records that belongs to provided
	 * TenantID.
	 * 
	 * @param accountPairRDD
	 *            :JavaPairRDD<String, String> where key represents accountID
	 *            and value is TenantID.
	 * @param tenantID
	 *            : TenantID
	 * @return : JavaPairRDD<String, String> that contains all the records
	 *         belongs to provided TenantID.
	 */
	public JavaPairRDD<String, String> getAccountRecordsforTenant(
			JavaPairRDD<String, String> accountPairRDD, final String tenantID) {
		return accountPairRDD
				.filter(new Function<Tuple2<String, String>, Boolean>() {

					@Override
					public Boolean call(Tuple2<String, String> tuple)
							throws Exception {
						return tuple._2().equals(tenantID);
					}
				});
	}

	/**
	 * Returns a JavaRDD containing UserProfile for each user. It will read
	 * JavaPairRDD containing TenantID,RegionID,ProfileID and AccountID returned
	 * from Join operation of Profile and Account RDDs
	 * 
	 * @param profileWithTenantRDD
	 *            : JavaPairRDD containing TenantID,RegionID,ProfileID and
	 *            AccountID returned from Join operation of Profile and Account
	 *            RDDs. This JavaPairRDD will have AccountID as a key and
	 *            Tuple2<String,String> as its value, which will contain
	 *            TenantID and Delimited String of RegionID and ProfileID
	 * @return JavaRDD of UserProfile.
	 */
	public JavaRDD<UserProfile> getUserProfile(
			JavaPairRDD<String, Tuple2<String, String>> profileWithTenantRDD) {

		return profileWithTenantRDD
				.map(new Function<Tuple2<String, Tuple2<String, String>>, UserProfile>() {

					@Override
					public UserProfile call(
							Tuple2<String, Tuple2<String, String>> tuple)
							throws Exception {
						UserProfile userProfile = new UserProfile();
						Tuple2<String, String> valueTuple = tuple._2();
						userProfile.tenantID = valueTuple._1();
						String[] profileVal = valueTuple._2().split(
								VALUE_DELIMETER);
						userProfile.userID = profileVal[0];
						userProfile.regionID = profileVal[1];
						return userProfile;
					}
				});
	}

	/**
	 * This method will assign a userProfile for each Recommended ItemID.
	 * 
	 * @param userRecommendationsRDD
	 *            : A PairRDD where key is UserProfile and Value is list of
	 *            Recommendations.
	 * @return : PairRDD where Key will be UserProfile and Value will be ItemID
	 *         recommended for that UserProfile.
	 */

	public JavaPairRDD<UserProfile, String> toUserProfilePerRecoRDD(
			JavaPairRDD<UserProfile, List<String>> userRecommendationsRDD) {

		return userRecommendationsRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<UserProfile, List<String>>, UserProfile, String>() {

					List<Tuple2<UserProfile, String>> recommendItem = new ArrayList<Tuple2<UserProfile, String>>();

					@Override
					public Iterable<Tuple2<UserProfile, String>> call(
							Tuple2<UserProfile, List<String>> record)
							throws Exception {
						List<String> recoList = record._2();
						UserProfile userProfile = record._1();
						for (String item : recoList) {
							recommendItem.add(new Tuple2<UserProfile, String>(
									userProfile, item));
						}

						return recommendItem;
					}
				});
	}
}
