package com.dla.foundation.crawler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.crawler.util.SparkCrawlerUtils;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CassandraConfig;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CrawlerConfig;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.GigyaConfig;
import com.dla.foundation.model.FriendsInfo;
import com.dla.foundation.model.FriendsInfoResponse.Friend;
import com.dla.foundation.model.Profile;
import com.dla.foundation.model.SocialProfile;
import com.dla.foundation.model.UserProfileResponse;

public class SocialMediaCrawler {

	private static final long NULLTHRESHOLDTIME = -1L;
	private static final String DEF_RELATION = "friend";
	private static final int DEF_DUMMYFLAG_VALUE = 1;

	public void runSocialMediaCrawler(String master, String appName,
			CrawlerConfig crawlerConf, CassandraConfig cassandraConf,
			GigyaConfig gigyaConf) {
		runSocialMediaCrawler(master, appName, crawlerConf, cassandraConf,
				gigyaConf, NULLTHRESHOLDTIME);
	}

	public void runSocialMediaCrawler(String master, String appName,
			CrawlerConfig crawlerConf, CassandraConfig cassandraConf,
			GigyaConfig gigyaConf, long outDatedThresholdTime) {

		String socialProfileOuputQuery = "UPDATE " + crawlerConf.keySpace + "."
				+ crawlerConf.socialProfileCF + " SET " + SocialProfile.name
				+ "=?," + SocialProfile.username + "=?,"
				+ SocialProfile.location + "=?," + SocialProfile.gender + "=?,"
				+ SocialProfile.country + "=?," + SocialProfile.timezone + "=?";

		String friendsInfoQuery = "UPDATE  " + crawlerConf.keySpace + "."
				+ crawlerConf.friendsCF + " SET " + FriendsInfo.relation + "=?";

		String lastModifedUpdateQuery = "UPDATE " + crawlerConf.keySpace + "."
				+ crawlerConf.profileCF + " SET " + crawlerConf.lastModifiedKey
				+ "=?";

		// Where clause query to get users for which last modified date is less
		// than outDatedThresholdTime
		String whereClause = Profile.dummyflag + " = " + DEF_DUMMYFLAG_VALUE
				+ " AND " + Profile.lastmodified + " < "
				+ outDatedThresholdTime;

		String relation = DEF_RELATION;

		Configuration conf = new Configuration();

		CassandraSparkConnector connector = new CassandraSparkConnector(
				cassandraConf.ipList, cassandraConf.inputPartitioner,
				cassandraConf.port, cassandraConf.ipList,
				cassandraConf.outputPartitioner);

		JavaSparkContext sparkContext = new JavaSparkContext(master, appName);

		try {
			runCrawler(sparkContext, connector, conf, crawlerConf, gigyaConf,
					cassandraConf.inputCQLPageRowSize, socialProfileOuputQuery,
					friendsInfoQuery, lastModifedUpdateQuery, whereClause,
					relation);
		} finally {
			sparkContext.stop();
		}
	}

	public void runCrawler(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraCon, Configuration conf,
			CrawlerConfig crawlerConf, GigyaConfig gigyaConf,
			String inputCQLPageRowSize, String socialProfileOuputQuery,
			String friendsInfoQuery, String lastModifedUpdateQuery,
			String whereClause, String relation) {

		// Reading profile table from Cassandra
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> profileRDD = cassandraCon
				.read(conf, sparkContext, crawlerConf.keySpace,
						crawlerConf.profileCF, inputCQLPageRowSize, whereClause);

		// Getting dla and social id from columnfamily (Cassandra)
		JavaPairRDD<String, String> userSocialPair = SparkCrawlerUtils
				.getIDLAandSocialID(profileRDD, crawlerConf.profileIdKey,
						crawlerConf.socialIdKey);

		SparkGigyaConnector gigyaConnector = new SparkGigyaConnector(
				gigyaConf.apiKey, gigyaConf.secretKey, gigyaConf.apiScheme,
				gigyaConf.apiDomain);

		// Calling Gigya to get social data for user
		JavaPairRDD<String, UserProfileResponse> rawsocialProfile = gigyaConnector
				.getSocialProfile(userSocialPair, gigyaConf.timeoutMillis);

		// Filtering null records
		JavaPairRDD<String, UserProfileResponse> socialProfile = CrawlerPostProcecssing
				.filterSocialProfile(rawsocialProfile);

		// Caching data otherwise the stream runs twice once for social profile
		// table update and second for profile table update
		socialProfile.cache();

		// Formatting stream for writing data to cassandra
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> formattedSocialProfile = CrawlerPostProcecssing
				.formatSocialProfileForCassandra(socialProfile);

		// Filtering null records, in case there is error to get details from
		// Gigya
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> filteredSocialProfile = SparkCrawlerUtils
				.filter(formattedSocialProfile);

		// Writing social data to Cassandra
		cassandraCon.write(conf, crawlerConf.keySpace,
				crawlerConf.socialProfileCF, socialProfileOuputQuery,
				filteredSocialProfile);

		// Calling gigya to get friends info
		JavaPairRDD<String, Friend> friendsInfo = gigyaConnector.getFriends(
				userSocialPair, gigyaConf.timeoutMillis);

		// PostProcessing data to resolve Friend's Social id to DLA user id
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> filteredFriendsInfo = CrawlerPostProcecssing
				.runFriendsInfoPostProcessing(sparkContext, cassandraCon,
						crawlerConf, friendsInfo, inputCQLPageRowSize, relation);

		// Writing friendsinfo to Cassandra
		cassandraCon.write(conf, crawlerConf.keySpace, crawlerConf.friendsCF,
				friendsInfoQuery, filteredFriendsInfo);

		// Writing updated timestamp to profile table after successful social
		// fetch. Currently I'm considering if social profile data fetch is
		// successful, then the records will be updated in cassandra without
		// fail. There are no guarantees here but no other way since cassandra
		// write does not return any stream after write
		long newLastModified = System.currentTimeMillis();

		// Preparing data for cassandra write
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> updatedProfile = CrawlerPostProcecssing
				.prepareForProfileUpdate(socialProfile, newLastModified);

		// Updating last modified of profile table in cassandra
		cassandraCon.write(conf, crawlerConf.keySpace, crawlerConf.profileCF,
				lastModifedUpdateQuery, updatedProfile);
	}

}
