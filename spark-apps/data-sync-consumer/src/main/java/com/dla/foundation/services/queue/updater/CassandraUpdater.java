package com.dla.foundation.services.queue.updater;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.DependencyLocator;
import com.dla.foundation.data.FoundationDataService;
import com.dla.foundation.data.FoundationDataServiceImpl;
import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.data.persistence.cassandra.CassandraContext;
import com.dla.foundation.analytics.utils.PropertiesHandler;

/**
 * Cassandra Specific updater.
 * Used this updater when the data is to be written to Cassandra.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class CassandraUpdater implements Updater {

	final Logger logger = Logger.getLogger(this.getClass());

	private final String DEFAULT_PROPERTIES_FILE_PATH = "src/main/resources/CassandraUpdater.properties";
	private final String PROPERTIES_FILE_VAR = "cupropertiesfile";
	String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR,DEFAULT_PROPERTIES_FILE_PATH);
	PropertiesHandler phandler;
	private String nodeIpList;	
	private String dataKeyspace;
	private String entityPackagePrefix;

	private static FoundationDataService dataService = null;

	private CassandraContext dataContext;

	public CassandraUpdater(DependencyLocator dependencyLocator) {
		CassandraUpdater.dataService = dependencyLocator.get(FoundationDataService.class);
	}

	public CassandraUpdater() {
		try {
			phandler = new PropertiesHandler(propertiesFilePath);
			nodeIpList = phandler.getValue("nodeIpList");
			dataKeyspace = phandler.getValue("dataKeyspace");
			entityPackagePrefix =  phandler.getValue("entityPackagePrefix");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String[] nodeIps = nodeIpList.split(",");
		dataContext = CassandraContext.create(entityPackagePrefix, dataKeyspace, nodeIps);
		CassandraUpdater.dataService= new FoundationDataServiceImpl(dataContext);
		logger.info("Connected to Cassandra Cluster");
	}

	@Override
	public void close() {

	}

	/**
	 * Write even to Cassandra and return the appropriate event object returned 
	 * by underlying Cassandra Writer
	 */
	@Override
	public AnalyticsCollectionEvent updateSyncEvent(
			AnalyticsCollectionEvent event) {
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
	 * This method does not return any acknowledgment or message to caller unlike updateSyncEvent method
	 */
	@Override
	public void updateAsyncEvent(AnalyticsCollectionEvent event) {
		try {
			dataService.insertOrUpdateCollectionEvent(event);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}