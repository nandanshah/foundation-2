package com.dla.foundation.pio;

import io.prediction.Client;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.pio.util.CassandraConfig;

public class SparkPIOConnector implements Serializable {

	private static final long serialVersionUID = 8073585091774712586L;
	private static final String CURRENT_TIME= "now";
	private static Logger logger = Logger.getLogger(SparkPIOConnector.class.getName());

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
	 * Get ALL recommendations for provided list of users from PIO with
	 * SparkContext. (This method will return empty list of recommendation for
	 * users - PIO not able to return results.)
	 * 
	 * @param allUsersRDD
	 *            list of Users for whose recommendations are required.
	 * @param strAppKey
	 *            AppKey that Client will use to communicate with API.
	 * @param strAppURL
	 *            AppURL that Client will use to communicate with API
	 * @param strEngine
	 *            EngineName that Client will use to get Recommendations.
	 * @param intNumRecPerUser
	 *            Maximum number of recommendations to be fetch per user.
	 * @return JavaPairRDD<String, List<String>> Key-Value pair where Key is
	 *         UserID and value is List of Recommendations.
	 */

	public JavaPairRDD<String, List<String>> getRecommendationsFromPIO(
			JavaRDD<String> allUsersRDD, final String strAppKey,
			final String strAppURL, final String strEngine,
			final int intNumRecPerUser) {
		logger.info("Started fetching recommendations for users from Prediction IO.");
		final JavaPairRDD<String, List<String>> recommendedData = allUsersRDD
				.mapToPair(new PairFunction<String, String, List<String>>() {

					private static final long serialVersionUID = 5528816445669906910L;

					@SuppressWarnings("finally")
					@Override
					public Tuple2<String, List<String>> call(String userId)
							throws Exception {
						String recommendationArray[] = {};
						Client client =null;
						try {
							// Currently PIO Client SDK is instantiated inside
							// call method because Client class does not
							// implements the Serilizable interface.
							client = new Client(strAppKey, strAppURL);
							client.identify(userId);
							logger.info("Fetching recommendations for user id " +userId);
							// gets intNumRecPerUser recommendations from PIO.
							recommendationArray = client.getItemRecTopN(
									strEngine, intNumRecPerUser);
							

						} catch (IOException e) {
							logger.warn(e.getMessage());
							// If PIO doesn't find recommendation for particular
							// user it throws IOException, in such cases we are
							// returning empty array for same user.

						} finally {
							client.close();

							return new Tuple2<String, List<String>>(userId,
									Arrays.asList(recommendationArray));
						}

					}

				});
		return recommendedData;

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

	public JavaPairRDD<String, List<String>> removeEmptyRecommendations(
			JavaPairRDD<String, List<String>> allRecommendations) {
		JavaPairRDD<String, List<String>> filteredRecommendations = allRecommendations
				.filter(new Function<Tuple2<String, List<String>>, Boolean>() {

					private static final long serialVersionUID = -7711009473196986893L;

					@Override
					public Boolean call(
							Tuple2<String, List<String>> recommendation)
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
	 * Returns JavaPairRDD in (<Map<String, ByteBuffer>,java.util.List<ByteBuffer>>) format that which is used for writing data
	 * to Cassandra.
	 * 
	 * @param filteredRecommendations
	 *            JavaPairRDD of users those have non-empty recommendation list.
	 * @param userID
	 *            ColuumnName of the PrimaryKey of Target Cassandra Table.
	 * @return JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>>
	 *         JavaPairRDD in special format that is suitable for writing data
	 *         to Cassandra.
	 */

	public JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>> formatRecommendationsForCassandraWrite(
			JavaPairRDD<String, List<String>> filteredRecommendations,
			final String userID) {

		JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>> cassandraRDD = filteredRecommendations
				.mapToPair(new PairFunction<Tuple2<String, List<String>>, Map<String, ByteBuffer>, java.util.List<ByteBuffer>>() {

					private static final long serialVersionUID = 8894826755888085258L;
					Map<String, ByteBuffer> keys;
					List<ByteBuffer> values;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<String, List<String>> recommendation)
							throws Exception {
						keys = new LinkedHashMap<String, ByteBuffer>();
						values = new ArrayList<ByteBuffer>();

						// This will convert UserID string to UUID format and
						// will put to keys MAP.
						keys.put(userID,
								UUIDType.instance.fromString(recommendation._1));
						
						ArrayList<UUID> newUUIDList = new ArrayList<UUID>();
						//Converting all movieids from String type to UUID.
						for(String str : recommendation._2){
							
							newUUIDList.add(UUIDType.instance.compose(UUIDType.instance.fromString(str)));
						}
						ListType list = ListType.getInstance(UUIDType.instance);
						// This will add all recommendations to ByteBuffer.
						values.add(list.decompose(newUUIDList));
						
						// This will add current timestamp.
						values.add(TimestampType.instance.fromString(CURRENT_TIME));
						return new Tuple2<Map<String, ByteBuffer>, java.util.List<ByteBuffer>>(
								keys, values);
					}

				});

		return cassandraRDD;
	}
	
	/**
	 * This method retrives userids from records read from Cassandra profile table. 
	 * @param allUsersRecords all user records read from Cassandra profile tables.
	 * @param cassandraConfig instance of Cassandra Config.
	 * @return 
	 */
	
	public JavaRDD<String> getUsersFromCassandraRecords(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> allUsersRecords,
			final CassandraConfig cassandraConfig) {
		JavaRDD<String> users = allUsersRecords
				.map(new Function<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String>() {

					private static final long serialVersionUID = -1641738530261115057L;

					@Override
					public String call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> dataFromCassandra)
							throws Exception {
						Map<String, ByteBuffer> keySet = dataFromCassandra._1();
					ByteBuffer ID= keySet.get(cassandraConfig.profilePrimaryKey);
					return UUIDType.instance.compose(ID).toString();
					}

				});
		return users;
	}
}
