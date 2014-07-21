package com.dla.foundation.crawler;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.crawler.util.CrawlerPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.crawler.util.SparkCrawlerUtils;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CassandraConfig;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.CrawlerConfig;
import com.dla.foundation.crawler.util.SparkCrawlerUtils.GigyaConfig;

/**
 * Sample Client class which contains main method to spawn the spark job
 * 
 */
public class CrawlerDriver {

	public static final String DEFAULT_PROPERTIES_FILE_PATH = "src/main/resources/crawler.properties";
	public static final String PROPERTIES_FILE_VAR = "propertiesfile";
	public static final long DEF_INITIAL_THREASHOLDTIME = 9999999999999L;

	private static Logger logger = Logger.getLogger(CrawlerDriver.class);

	public static void main(String[] args) throws IOException {
		String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR,
				DEFAULT_PROPERTIES_FILE_PATH);
		CrawlerDriver driver = new CrawlerDriver();
		long outDatedThresholdTime = DEF_INITIAL_THREASHOLDTIME;
		driver.run(propertiesFilePath, outDatedThresholdTime);
	}

	public void run(String propertiesFilePath, long outDatedThresholdTime)
			throws IOException {

		PropertiesHandler phandler = null;
		try {
			phandler = new PropertiesHandler(propertiesFilePath);
		} catch (IOException e) {
			logger.fatal("Error getting properties file", e);
			throw e;
		}

		// Initializing values from properties file
		String master = phandler.getValue(CrawlerPropKeys.sparkMaster
				.getValue());
		String appName = phandler.getValue(CrawlerPropKeys.sparkApName
				.getValue());

		CassandraConfig cassandraConf = SparkCrawlerUtils
				.initCassandraConfig(phandler);

		GigyaConfig gigyaConf = SparkCrawlerUtils.initGigyaConfig(phandler);

		CrawlerConfig crawlerConf = SparkCrawlerUtils
				.initCrawlerConfig(phandler);

		logger.info("Starting Crawler with input properties file: "
				+ propertiesFilePath + " with outdatedTimeThreshold: "
				+ outDatedThresholdTime);

		SocialMediaCrawler crawler = new SocialMediaCrawler();

		crawler.runSocialMediaCrawler(master, appName, crawlerConf,
				cassandraConf, gigyaConf, outDatedThresholdTime);

		logger.info("Social Media Crawler run complete");
	}

}
