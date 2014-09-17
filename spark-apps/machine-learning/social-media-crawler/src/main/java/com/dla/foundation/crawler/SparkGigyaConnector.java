package com.dla.foundation.crawler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.model.FriendsInfoResponse;
import com.dla.foundation.model.FriendsInfoResponse.Friend;
import com.dla.foundation.model.UserProfileResponse;
import com.gigya.socialize.GSException;

public class SparkGigyaConnector implements Serializable {

	private static final long serialVersionUID = 5919588303249120048L;
	private final GigyaConnectorApi gigya;
	private static Logger logger = Logger.getLogger(SparkGigyaConnector.class);

	public SparkGigyaConnector(String apiKey, String secretKey,
			String apiScheme, String apiDomain) {
		gigya = new GigyaConnectorApi(apiKey, secretKey, apiScheme, apiDomain);
	}

	/**
	 * Gets social profile from Gigya using input userid (String) Input is in
	 * the format <K,V> as <dla_id, social_id> and output is <K,V> <dlaid,
	 * UserProfileResponse>
	 * 
	 * @param pairs
	 * @param timeout
	 * @return social profile information in cassandra format
	 */
	public JavaPairRDD<String, UserProfileResponse> getSocialProfile(
			JavaPairRDD<String, String> pairs, final int timeout) {

		JavaPairRDD<String, UserProfileResponse> userProfileRDD = pairs
				.mapToPair(new PairFunction<Tuple2<String, String>, String, UserProfileResponse>() {
					private static final long serialVersionUID = -1252569529998566825L;

					@Override
					public Tuple2<String, UserProfileResponse> call(
							Tuple2<String, String> tuple) throws Exception {

						String uid = tuple._1;
						String userid = tuple._2;
						
						UserProfileResponse userProfileResponse = null;
						// Calling Gigya to get Social Profile
						logger.info("Calling Gigya to get User Profile for userid: "
								+ userid);
						try {
							userProfileResponse = gigya.getUserInfo(userid,
									timeout);
						} catch (GSException e) {
							logger.error(
									"Error while fetching data from Gigya "
											+ e.getMessage(), e);
							return null;
						}

						return new Tuple2<String, UserProfileResponse>(uid,
								userProfileResponse);
					}
				});
		return userProfileRDD;
	}

	/**
	 * Gets friends information from Gigya using input userid (String) Input is
	 * in the format <K,V> as <dla_id, social_id> and output is in format <K,V>
	 * <dlaid, Friend>
	 * 
	 * @param pairs
	 * @param timeout
	 * @return
	 */
	public JavaPairRDD<String, Friend> getFriends(
			JavaPairRDD<String, String> pairs, final int timeout) {

		JavaPairRDD<String, Friend> friendsInfo = pairs
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, Friend>() {

					private static final long serialVersionUID = -1982180058742299170L;
					String userid = null;
					String profileId = null;
					List<Tuple2<String, Friend>> friendsList = new ArrayList<Tuple2<String, Friend>>();

					@Override
					public Iterable<Tuple2<String, Friend>> call(
							Tuple2<String, String> tuple) throws Exception {
						profileId = tuple._1;
						userid = tuple._2;
						try {
							// Calling Gigya to get Friends info
							logger.info("Calling Gigya to get Friends infor for userid: "
									+ userid);
							FriendsInfoResponse response = gigya.getFriends(
									userid, false, timeout);

							for (Friend friend : response.friends) {
								friendsList.add(new Tuple2<String, Friend>(
										profileId, friend));
							}

						} catch (GSException e) {
							logger.error(
									"Error while fetching data from Gigya "
											+ e.getMessage(), e);
						}
						return friendsList;
					}
				});
		return friendsInfo;
	}
}
