package com.dla.foundation.socialReco;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import com.dla.foundation.socialReco.util.FriendsInfoTransformation;
import com.dla.foundation.socialReco.util.ProfileTransformation;
import com.dla.foundation.socialReco.util.PropKeys;
import com.dla.foundation.socialReco.util.SocialRecoProp;
import com.dla.foundation.socialReco.util.SocialRecommendationUtil;
import com.dla.foundation.socialReco.model.CassandraConfig;
import com.dla.foundation.socialReco.model.Social;
import com.dla.foundation.socialReco.model.UserScore;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.analytics.utils.CommonPropKeys;

/**
 * Sample Client class which contains main method to spawn the spark job
 * 
 * @author prajakta_bhosale
 */

public class SocialScoreDriver  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	private static Logger logger = Logger.getLogger(SocialScoreDriver.class);
	private static final String TRUE = "true";

	public static void main(String[] args) throws Exception {
		if (args.length == 1) {
			String commPropFilePath = args[0];
			SocialScoreDriver driver = new SocialScoreDriver();
			driver.run(commPropFilePath);
		} else {
			System.err.println("USAGE: CommonPropertiesfile");
		}
	}

	/**
	 * 
	 * This method will initialize the Property Handler, spark context, Cassandra Service
	 *  
	 * @param commPropFilePath
	 *            : Path of common application properties having properties
	 *            read_host_list ,write_host_list,input_partitioner,
	 *            output_partitioner ,rpc_port.
	 *  
	 */
	public void run(String commPropFilePath)
			throws Exception{

		//PropertiesHandler  socialScoreProp = null;
		
			final PropertiesHandler  socialScoreProp = new PropertiesHandler(					
					commPropFilePath, SocialRecoProp.SOCIAL_SCORE_APP_NAME);

			// initializing spark context
			logger.info("initializing spark context");
			JavaSparkContext sparkContext = new JavaSparkContext(
					socialScoreProp.getValue(CommonPropKeys.spark_host.getValue()), 
					SocialRecoProp.SOCIAL_SCORE_APP_NAME);

			// initializing cassandra service
			logger.info("initializing cassandra service");
			CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
					SocialRecommendationUtil.getList(socialScoreProp.
							getValue(CommonPropKeys.cs_hostList.getValue()), ","),
							SocialRecoProp.PARTITIONER,
							socialScoreProp.getValue(CommonPropKeys.cs_rpcPort.getValue()),
							SocialRecommendationUtil.getList(socialScoreProp
									.getValue(CommonPropKeys.cs_hostList.getValue()),","), 
									SocialRecoProp.PARTITIONER);

			runSocialScoreDriver(sparkContext, cassandraSparkConnector,
					socialScoreProp);
		
	}

	/**
	 * This method will initialize the cassandra config for all the 3 tables(common_daily_eventsummary_per_useritem,
	 *  social_friends_info, profile).Fetch the data from tables, perform the operations, Calculate the Social 
	 *  Recommendation for users and write those recommendations to the social_reco table.
	 * 
	 * @param sparkContext
	 *            : spark context intialized with mode and name.
	 * @param cassandraService
	 *            : cassandra context intialized with input and output ips and
	 *            partitioner.
	 * @param socialScoreProp
	 *            : Property handler to read the application properties like 
	 *            app_name,read_host_list
	 *            ,write_host_list,input_partitioner,output_partitioner
	 *            ,rpc_port.
	 * 
	 * @throws Exception
	 *            
	 *            
	 */
	public void runSocialScoreDriver(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			final PropertiesHandler socialScoreProp) throws Exception {

		final String SOCIAL_SCORE_QUERY_PROPERTY ;
		try {

			logger.info("initializing query for SocialScore");
			SOCIAL_SCORE_QUERY_PROPERTY = "UPDATE "
					+ socialScoreProp.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()) + "."
							+ SocialRecoProp.SOCIAL_SCORE_OUT_CF + " SET "
							+ Social.SOCIAL_SCORE.getColumn() + " =?, "
							+ Social.SOCIAL_SCORE_REASON.getColumn() + " =?,"
							+ Social.DATE.getColumn() + "=?,"
							+ Social.EVENTREQUIRED.getColumn() + "=?"; 

			Date socialRecoDate ; 
			if (socialScoreProp.getValue(PropKeys.SOCIAL_RECO_DATE_FLAG.getValue()).
					toLowerCase().compareTo(TRUE) == 0) {
				// Get the input date from eo_spark_app_prop columnfamily
				socialRecoDate = SocialRecommendationUtil.getDate(socialScoreProp
					.getValue(PropKeys.SOCIAL_RECO_DATE
							.getValue()), DATE_FORMAT);
			}else {
				
				socialRecoDate = SocialRecommendationUtil.getDate(socialScoreProp
						.getValue(PropKeys.RECAL_END_DATE
								.getValue()), DATE_FORMAT);
				
			}

			// Convert the date into timestamp 
			final long timestamp = socialRecoDate.getTime();

			// initializing cassandra config for initializing cassandra config for common_daily_eventsummary_per_useritem columnfamily
			logger.info("initializing cassandra config for common_daily_eventsummary_per_useritem columnfamily");
			CassandraConfig socialScoreCassandraProp = new CassandraConfig(
					socialScoreProp.getValue(CommonPropKeys.cs_fisKeyspace.getValue()),
					socialScoreProp.getValue(CommonPropKeys.cs_fisKeyspace.getValue()),
					SocialRecoProp.USER_EVENT_SUM_INP_CF,
					null,
					socialScoreProp.getValue(CommonPropKeys.cs_pageRowSize.getValue()),
					null);

			JavaPairRDD<String, UserScore> userScore = SocialRecommendationUtil.readUserEventSummaryData(
					sparkContext, cassandraSparkConnector, socialScoreCassandraProp, socialScoreProp);

			// initializing cassandra config for initializing cassandra config for social_friends_info columnfamily
			logger.info("initializing cassandra config for social_friends_info columnfamily");
			CassandraConfig friendsInfoCassandraProp = new CassandraConfig(
					socialScoreProp.getValue(CommonPropKeys.cs_fisKeyspace.getValue()),
					socialScoreProp.getValue(CommonPropKeys.cs_fisKeyspace.getValue()),
					SocialRecoProp.FRIENDSINFO_INP_CF,
					null,
					socialScoreProp.getValue(CommonPropKeys.cs_pageRowSize.getValue()),
					null);

			JavaPairRDD<String, String> friendsInfo = FriendsInfoTransformation.
					readFriendsInfoData(sparkContext, cassandraSparkConnector, friendsInfoCassandraProp);

			String considerSelfReco = socialScoreProp.getValue(PropKeys.CONSIDER_SELF_RECO.getValue());
		
			JavaPairRDD<String, UserScore>  dayScoreRDDPerUserMovie = FriendsInfoCalc.scoreCalculations(
					userScore, friendsInfo,considerSelfReco);

			// initializing cassandra config for profile columnfamily and socialScore
			logger.info("initializing cassandra config for profile columnfamily and socialScore");
			CassandraConfig profileCassandraProp = new CassandraConfig(
					socialScoreProp.getValue(CommonPropKeys.cs_platformKeyspace.getValue()),
					socialScoreProp.getValue(CommonPropKeys.cs_fisKeyspace.getValue()),
					SocialRecoProp.PROFILE_INP_CF,
					SocialRecoProp.SOCIAL_SCORE_OUT_CF,
					socialScoreProp.getValue(CommonPropKeys.cs_pageRowSize.getValue()),
					SOCIAL_SCORE_QUERY_PROPERTY);

			final Integer topEntryCnt = Integer.parseInt(socialScoreProp.getValue(PropKeys.TOP_ENTRIES_COUNT.getValue()));

			JavaPairRDD<String, String> profileInfo = ProfileTransformation.readProfileData(sparkContext,
					cassandraSparkConnector, profileCassandraProp);
			
			JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = ProfileCalc.regionCalculations(
					dayScoreRDDPerUserMovie,profileInfo, topEntryCnt, timestamp );
			
			
			Configuration conf= new Configuration();

			// Writing records to social_reco columnfamily
			logger.info("Writing records to social_reco columnfamily ");
			cassandraSparkConnector.write(conf,
					profileCassandraProp.getOutputKeyspace(),
					profileCassandraProp.getOutputColumnfamily(),
					profileCassandraProp.getOutputQuery(), cassandraOutputRDD);

			if (socialScoreProp.getValue(PropKeys.SOCIAL_RECO_DATE_FLAG.getValue()).
					toLowerCase().compareTo(TRUE) == 0) {
				logger.info("Updating Social Reco Date");
				// Change the Input date by one day
				Date newInputDateSocialReco = DateUtils.addDays(
						SocialRecommendationUtil.getDate(socialScoreProp
								.getValue(PropKeys.SOCIAL_RECO_DATE
										.getValue()), DATE_FORMAT), 1);

				// Write new input date to eo_spark_app_prop columnfamily
				socialScoreProp.writeToCassandra(PropKeys.SOCIAL_RECO_DATE
						.getValue(), SocialRecommendationUtil.getDate(
								newInputDateSocialReco, DATE_FORMAT));
			}
			logger.info("-----------Social Recommendation DONE---------");

		} catch (Exception e) {
			logger.error(e);
		}
	}


}


