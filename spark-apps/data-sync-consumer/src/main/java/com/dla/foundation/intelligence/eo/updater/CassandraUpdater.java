package com.dla.foundation.intelligence.eo.updater;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

import com.dla.foundation.DependencyLocator;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.FoundationDataService;
import com.dla.foundation.data.FoundationDataServiceImpl;
import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.data.persistence.cassandra.CassandraContext;
import com.dla.foundation.intelligence.eo.filter.Filter;
import com.dla.foundation.intelligence.eo.filter.FilterException;

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
	private String PROPERTIES_FILE_NAME = "common.properties";
	private String PROPERTIES_FILE_VAR = "commonproperties";
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
			nodeIpList = phandler.getValue(CommonPropKeys.cs_hostList);	
			dataKeyspace = phandler.getValue(CommonPropKeys.cs_fisKeyspace);
			entityPackagePrefix = phandler.getValue(CommonPropKeys.cs_entityPackagePrefix);
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}

		String[] nodeIps = nodeIpList.split(",");
		dataContext = CassandraContext.create(entityPackagePrefix, dataKeyspace, nodeIps);
		CassandraUpdater.dataService= new FoundationDataServiceImpl(dataContext);
		logger.info("Connected to Cassandra Cluster");
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> TEntity filterEvent(TEntity event,
			ArrayList<Filter> filters) throws FilterException {
		for (Filter filter : filters) {
			event = filter.doFilter(event);
		}
		return event;
	}

	/**
	 * Write even to Cassandra and return the appropriate event object returned 
	 * 
	 * by underlying Cassandra Writer
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <TEntity extends SimpleFoundationEntity> TEntity doUpdateSyncEvent(
			TEntity event) {
		UserEvent ret = null;
		try {
			ret =  dataService.insertOrUpdateUserEvent((UserEvent) event);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return (TEntity) ret;
	}

	/**
	 * Write even to Cassandra. 
	 * 
	 * This method does not return any acknowledgment or message to caller unlike updateSyncEvent method
	 */
	@Override
	protected <TEntity extends SimpleFoundationEntity> void doUpdateAsyncEvent(
			TEntity event) {
		try {
			dataService.insertOrUpdateUserEvent((UserEvent) event);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void close() {
		try {
			dataContext.close();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
}