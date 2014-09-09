package com.dla.foundation.trendReco.services;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.DayScore;
import com.dla.foundation.trendReco.model.TrendScore;

/**
 * This class will calculate zscore for the items. zscore = (current trend -
 * average)/standard deviation. current trend is day score for specified date.
 * Standard deviation = sqrt(sqr(sum(diff of each element with average)))
 * 
 * @author shishir_shivhare
 * @version: 1.0
 * @since 2014-06-21
 */
public class ZScoreService implements Serializable, ITrendScore {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7875760927306722697L;
	private static final Logger logger = Logger.getLogger(ZScoreService.class);
	private static final String TRENDING = "trending";
	private static final String REASON_NA = "NA";
	/**
	 * 
	 */
	// Date which will be consider for current trend.
	private long currentTrendDate;
	// period for which data will be taken to calcualte average and standard
	// deviation from the current trend.
	private int daysForHistoricTrend;

	public void setCurrentTrendDate(long currentTrendDate) {
		this.currentTrendDate = currentTrendDate;
	}

	public long getCurrentTrendDate() {
		return currentTrendDate;
	}

	public int getDaysForHistoricTrend() {
		return daysForHistoricTrend;
	}

	public ZScoreService(Long currentTrendDate, int daysForHistoricTrend) {
		this.currentTrendDate = currentTrendDate;
		this.daysForHistoricTrend = daysForHistoricTrend;
	}

	/**
	 * This function will provide the functionality of calculating the zscore.
	 * 
	 * @param itemDayScoreRDD
	 *            : It takes key as trendid#regionid#itemid and value as day
	 *            score with complete information of record.
	 * @return
	 */
	public JavaRDD<TrendScore> calculateScore(
			JavaPairRDD<String, DayScore> itemDayScoreRDD) {
		logger.info("Zscore service: Grouping records on the basis of item");
		// apply group by logic to get all the items of same key in list.key is
		// trendid#regionid#itemid
		JavaPairRDD<String, Iterable<DayScore>> itemDayScoreRDDGroupedByKey = itemDayScoreRDD
				.distinct().groupByKey();
		// zscore logic
		logger.info("Zscore service: Calculating zscore");
		JavaRDD<TrendScore> itemTrendScoreRDD = itemDayScoreRDDGroupedByKey
				.map(new Function<Tuple2<String, Iterable<DayScore>>, TrendScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -3557883813981416382L;
					Iterator<DayScore> itemDayScoreIterator;
					DayScore itemDayScore;
					TrendScore itemTrendScore;

					public TrendScore call(
							Tuple2<String, Iterable<DayScore>> records)
							throws Exception {
						List<DayScore> itmeDaysScores = new ArrayList<DayScore>();
						double average = 0.0;
						double standardDeviation = 0.0;
						double sum = 0.0;
						double currentTrendItemDayScore = -1;
						int totalDays = 0;
						itemDayScoreIterator = records._2.iterator();

						while (itemDayScoreIterator.hasNext()) {

							itemDayScore = itemDayScoreIterator.next();

							if (currentTrendDate == itemDayScore.getTimestamp()) {
								currentTrendItemDayScore = itemDayScore
										.getDayScore();

							} else {
								sum += itemDayScore.getDayScore();
								itmeDaysScores.add(itemDayScore);
								totalDays++;

							}
						}
						if (-1 == currentTrendItemDayScore) {
							currentTrendItemDayScore = 0;
						}
						if (0 != totalDays) {
							average = (sum / totalDays);
						} else {
							average = 0;
						}
						standardDeviation = getStandardDeviation(
								itmeDaysScores, average);
						if (null != itemDayScore) {
							double zscore = getZScore(average,
									standardDeviation, currentTrendItemDayScore);
							if (zscore > 0) {
								itemTrendScore = new TrendScore(itemDayScore
										.getTenantId(), itemDayScore
										.getRegionId(), itemDayScore
										.getItemId(),TRENDING,
										zscore, currentTrendDate);
							} else {
								itemTrendScore = new TrendScore(itemDayScore
										.getTenantId(), itemDayScore
										.getRegionId(), itemDayScore
										.getItemId(), REASON_NA,
										zscore, currentTrendDate);
							}
						}
						return itemTrendScore;

					}
				});

		return itemTrendScoreRDD;
	}

	private double getStandardDeviation(List<DayScore> itemDayScores,
			double average) {
		double sum = 0;
		for (DayScore itemDayScore : itemDayScores) {
			sum += Math.pow((itemDayScore.getDayScore() - average), 2);
		}
		return Math.sqrt(sum);
	}

	/**
	 * This function provides the logic to calculate the zscore by considering
	 * the following case into consideration below: 1. Suppose the new movie is
	 * release it will have both average and standard deviation as 0.It will
	 * provide 0.5 value. Its because we cannot say new movie is in trend. It is
	 * released recently. 2. If standard deviation is only 0 then it means it 1
	 * day old movie. So we will use (currentValue - average) as involvement of
	 * standard deviation will make it infinite. As standard deviation is used
	 * to robust zscore we can avoid it if it is 0. 3. If all parameter are non
	 * zero then use the specified the formula to calculate zscore.
	 * 
	 * @param average
	 * @param standardDeviation
	 * @param currentValue
	 *            : value current trend.
	 * @return
	 */
	private double getZScore(double average, double standardDeviation,
			double currentValue) {
		if (0 == average && 0 == standardDeviation) {
			return 0.5;
		} else if (0 == standardDeviation) {
			return (currentValue - average);
		}
		return ((currentValue - average) / standardDeviation);
	}

	/**
	 * This method provides the functionality of calculating normalized score
	 * for the respective trend score. It has internal mechanism to calculate
	 * max trend score and using it to compute normalized score.
	 */
	@Override
	public JavaRDD<TrendScore> normalizedScore(
			JavaRDD<TrendScore> itemTrendScoreRDD) {
		JavaRDD<TrendScore> normalizedItemTrendScoreRDD = null;

		try {
			logger.info("Zscore service: Finding Max Score");
			TrendScore maxTrendScore = getMaxScore(itemTrendScoreRDD);
			logger.info("Zscore service: Calculating normalized score");
			normalizedItemTrendScoreRDD = computeNormalizedScore(
					itemTrendScoreRDD, maxTrendScore);
		} catch (UnsupportedOperationException e) {
			normalizedItemTrendScoreRDD = itemTrendScoreRDD
					.map(new Function<TrendScore, TrendScore>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 3089581636584422870L;
						/**
				 * 
				 */

						double trendScore;

						public TrendScore call(TrendScore record)
								throws Exception {

							trendScore = record.getTrendScore();
							record.setNormalizedScore((trendScore));

							return record;
						}

					});
		}

		return normalizedItemTrendScoreRDD;

	}

	/**
	 * 
	 * @param itemTrendScoreRDD
	 * @return Max score among all the trend score to normalized the score.
	 */
	private TrendScore getMaxScore(JavaRDD<TrendScore> itemTrendScoreRDD) {
		// lOGIC to find max SCORE
		TrendScore maxTrendScore = itemTrendScoreRDD
				.reduce(new Function2<TrendScore, TrendScore, TrendScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 954862327757240398L;

					public TrendScore call(TrendScore record1,
							TrendScore record2) throws Exception {
						if (record1.getTrendScore() > record2.getTrendScore()) {
							return record1;
						}

						return record2;

					}
				});
		return maxTrendScore;
	}

	/**
	 * It provides the functionality of computing normalized score for
	 * respective trend score.
	 * 
	 * @param itemTrendScoreRDD
	 * @param maxTrendScore
	 * @return trend score with its normalized score.
	 */
	private JavaRDD<TrendScore> computeNormalizedScore(
			JavaRDD<TrendScore> itemTrendScoreRDD,
			final TrendScore maxTrendScore) {
		JavaRDD<TrendScore> normalizedItemTrendScoreRDD = itemTrendScoreRDD
				.map(new Function<TrendScore, TrendScore>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 5709712840353707995L;
					double trendScore;

					public TrendScore call(TrendScore record) throws Exception {
						double maxZscore = maxTrendScore.getTrendScore();
						trendScore = record.getTrendScore();
						if (maxZscore != 0) {
							if (maxZscore > 0) {
								record.setNormalizedScore((trendScore / maxZscore));
							} else {
								maxZscore = maxZscore * -1;
								record.setNormalizedScore((trendScore / maxZscore));
							}
						} else {
							record.setNormalizedScore(trendScore);
						}
						return record;
					}

				});
		return normalizedItemTrendScoreRDD;

	}

}
