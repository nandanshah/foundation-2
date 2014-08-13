package com.dla.foundation.services.queue.updater;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

import com.dla.foundation.DependencyLocator;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.FoundationDataService;
import com.dla.foundation.data.FoundationDataServiceImpl;
import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.data.persistence.cassandra.CassandraContext;
import com.dla.foundation.services.queue.filter.Filter;

/**
 * Cassandra Specific updater.
 * Used this updater when the data is to be written to Cassandra.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class CassandraUpdater extends Updater {

	final Logger logger = Logger.getLogger(this.getClass());
	private static FoundationDataService dataService = null;
	private String PROPERTIES_FILE_NAME = "CassandraUpdater.properties";
	private String PROPERTIES_FILE_VAR = "cupropertiesfile";
	private String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);
	private CassandraContext dataContext;

	public CassandraUpdater(DependencyLocator dependencyLocator) {
		CassandraUpdater.dataService = dependencyLocator.get(FoundationDataService.class);
	}

	public CassandraUpdater() {
		if(propertiesFilePath == null)
			propertiesFilePath = SparkFiles.get(PROPERTIES_FILE_NAME);

		String entityPackagePrefix = null;
		String nodeIpList = null;
		String dataKeyspace = null;

		try {
			PropertiesHandler phandler = new PropertiesHandler(propertiesFilePath);
			nodeIpList = phandler.getValue("nodeIpList");	
			dataKeyspace = phandler.getValue("dataKeyspace");
			entityPackagePrefix = phandler.getValue("entityPackagePrefix");
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}

		String[] nodeIps = nodeIpList.split(",");
		dataContext = CassandraContext.create(entityPackagePrefix, dataKeyspace, nodeIps);
		CassandraUpdater.dataService= new FoundationDataServiceImpl(dataContext);
		logger.info("Connected to Cassandra Cluster");
	}

	@Override
	protected void filterEvent(AnalyticsCollectionEvent event,
			ArrayList<Filter> filters) {
		for (Filter filter : filters) {
			filter.doFilter(event);
		}
	}

	/**
	 * Write even to Cassandra and return the appropriate event object returned 
	 * 
	 * by underlying Cassandra Writer
	 */
	@Override
	protected AnalyticsCollectionEvent doUpdateSyncEvent(AnalyticsCollectionEvent event) {
		AnalyticsCollectionEvent ret = null;
		try {
			ret =  dataService.insertOrUpdateCollectionEvent(event);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return ret;
	}

	/**
	 * Write even to Cassandra. 
	 * 
	 * This method does not return any acknowledgment or message to caller unlike updateSyncEvent method
	 */
	@Override
	protected void doUpdateAsyncEvent(AnalyticsCollectionEvent event) {
		try {
			dataService.insertOrUpdateCollectionEvent(event);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void close() {

	}
}