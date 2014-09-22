package com.dla.foundation.crawler;

import java.io.IOException;

import com.dla.foundation.analytics.utils.CommonPropKeys;

import org.apache.log4j.Logger;

import com.dla.foundation.crawler.util.CrawlerPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.crawler.util.CrawlerStaticPropKeys;
import com.dla.foundation.crawler.util.SparkCrawlerUtils;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CassandraConfig;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CrawlerConfig;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.GigyaConfig;

/**
 * Sample Client class which contains main method to spawn the spark job
 * 
 */
public class CrawlerDriver {

	public static final long DEF_INITIAL_THREASHOLDTIME = 9999999999999L;
	public static final String appName = "social-media-crawler";
	public static final String LAST_CRAWLER_RUN_KEY = "lastcrawlerruntime";
	
	private static Logger logger = Logger.getLogger(CrawlerDriver.class);

	public static void main(String[] args) throws IOException {
		if (args.length == 1) {
			String propertiesFilePath = args[0];
			CrawlerDriver driver = new CrawlerDriver();
			driver.run(propertiesFilePath);
		} else {
			System.err.println("USAGE: CrawlerDriver propertiesfile lastoudatedtime[optional]");
		}
	}

	public void run(String propertiesFilePath)
			throws IOException {

		PropertiesHandler phandler = null;
		try {
			phandler = new PropertiesHandler(propertiesFilePath, CrawlerStaticPropKeys.SOCIAL_MEDIA_CRAWLER_APP_NAME);
		
			long outDatedThresholdTime = Long.parseLong(phandler
					.getValue(CrawlerPropKeys.OUT_DATED_THRESHOLD_TIME
							.getValue()));
				
			// Initializing values from properties file
			String master = phandler.getValue(CommonPropKeys.spark_host
					.getValue());

			CassandraConfig cassandraConf = SparkCrawlerUtils
					.initCassandraConfig(phandler);

			GigyaConfig gigyaConf = SparkCrawlerUtils.initGigyaConfig(phandler);

			CrawlerConfig crawlerConf = SparkCrawlerUtils
					.initCrawlerConfig(phandler);
		
			if(outDatedThresholdTime == DEF_INITIAL_THREASHOLDTIME){
				String lastruntime = phandler.getValue(LAST_CRAWLER_RUN_KEY);
				if(lastruntime != null)
					outDatedThresholdTime = Long.parseLong(lastruntime);
			}
		

			logger.info("Starting Crawler with input properties file: "
					+ propertiesFilePath + " with outdatedTimeThreshold: "
					+ outDatedThresholdTime);

			SocialMediaCrawler crawler = new SocialMediaCrawler();

			crawler.runSocialMediaCrawler(master, appName, crawlerConf,
					cassandraConf, gigyaConf, outDatedThresholdTime);
		
			phandler.writeToCassandra(LAST_CRAWLER_RUN_KEY, String.valueOf(System.currentTimeMillis()));
		
			logger.info("Social Media Crawler run complete");
		} catch (IOException e) {
			logger.fatal("Error getting properties file", e);
			throw e;
		}catch (Exception e) {
                        logger.error(e);
                        throw e;
                } finally {
                        phandler.close();
                }

	}

}
