package com.dla.foundation.useritemreco;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.util.Filter;
import com.dla.foundation.useritemreco.util.ScoreSummaryTransformation;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;
import com.google.common.base.Optional;

/**
 * This class is used to convert item level summary to user item level summary
 * by performing left outer between profile and score summary and in future, it
 * will also provide the functionality of performing left join with social reco
 * and pio reco.
 * 
 * @author shishir_shivhare
 * 
 */
public class UserItemSummaryCalc implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3133298652347309107L;
	private String itemLevelCFKeyspace;
	private String scoreSummaryCF;
	private String pageRowSize;
	private Date inputDate;
	private static final Logger logger = Logger
			.getLogger(UserItemSummaryCalc.class);

	public UserItemSummaryCalc(String itemLevelCFKeyspace,
			String scoreSummaryCF, String pageRowSize, Date inputDate) {
		super();
		this.itemLevelCFKeyspace = itemLevelCFKeyspace;
		this.scoreSummaryCF = scoreSummaryCF;
		this.pageRowSize = pageRowSize;
		this.inputDate = inputDate;
	}

	/**
	 * 
	 * This function is used to convert item level summary to user item level summary
	 * by performing left outer between profile and score summary and in future, it
	 * will also provide the functionality of performing left join with social reco
	 * and pio reco.
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param profileRDD
	 * @return
	 * @throws Exception
	 */
	public JavaRDD<UserItemSummary> calculateUserItemSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> profileRDD) throws Exception {
		logger.info("peforming left outer join between profile and score summary");
		JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> profileItemRDD = joinItemScoreSummary(
				sparkContext, cassandraSparkConnector, profileRDD);
		
		logger.info("filtering record");
		// to avoid those user whose region and tenant doesnot match with any of the region and tenant provided through item level reco(score summary).
		JavaRDD<UserItemSummary> filteredUserItemSummaryRDD = Filter
				.filterScoreSummary(getUserItemSummary(profileItemRDD));
		return filteredUserItemSummaryRDD;

	}

	/**
	 * This function will provide the functionality of performing left join of profile with
	 * item level column family(score summary).
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param profileRDD
	 * @return
	 * @throws Exception
	 */
	private JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> joinItemScoreSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> profileRDD) throws Exception {
		if (null != scoreSummaryCF && "" != scoreSummaryCF) {
			Configuration conf = new Configuration();
			logger.info("reading from item summary column family");
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraScoreSummaryRDD = cassandraSparkConnector
					.read(conf, sparkContext, itemLevelCFKeyspace,
							scoreSummaryCF, pageRowSize,
							UserItemRecommendationUtil
									.getWhereClause(inputDate));
			logger.info("transforming item summary column family");
			JavaPairRDD<String, ItemSummary> scoreSummaryRDD = ScoreSummaryTransformation
					.getScoreSummary(cassandraScoreSummaryRDD);
			
			logger.info("filtering item summary  column family");
			JavaPairRDD<String, ItemSummary> filteredScoreSummaryRDD = Filter
					.filterItemSummary(scoreSummaryRDD);
			
			logger.info("performing left outer join between profile and item summary  column family");
			return profileRDD.leftOuterJoin(filteredScoreSummaryRDD);

		} else {
			throw new Exception(
					"column family name for score summary is not proper");
		}
	}

	/**
	 * This function will combine the result of all the join into user item summary.
	 * @param profileItemRDD
	 * @return
	 */
	private JavaRDD<UserItemSummary> getUserItemSummary(
			JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> profileItemRDD) {
		logger.info("combining the result of all the join into user item summary.");
		JavaRDD<UserItemSummary> userItemSummaryRDD = profileItemRDD
				.map(new Function<Tuple2<String, Tuple2<String, Optional<ItemSummary>>>, UserItemSummary>() {
					/**
		 * 
		 */
					private static final long serialVersionUID = 7819779796058281951L;
					UserItemSummary userItemSummary;

					public UserItemSummary call(
							Tuple2<String, Tuple2<String, Optional<ItemSummary>>> record)
							throws Exception {
						String userId = record._2._1;

						if (record._2._2.isPresent()) {
							userItemSummary = new UserItemSummary(userId,
									record._2._2.get());
							return userItemSummary;
						}

						return null;
					}
				});

		return userItemSummaryRDD;
	}

}
