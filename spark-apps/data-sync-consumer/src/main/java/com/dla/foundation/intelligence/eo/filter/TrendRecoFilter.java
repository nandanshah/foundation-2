package com.dla.foundation.intelligence.eo.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkFiles;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.intelligence.entity.UserEvent;
import com.dla.foundation.data.entities.event.EventType;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

/**
 * Sets a flag in UserEvent to indicate whether that event
 * is required for Trend recommendation calculation or not.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class TrendRecoFilter implements Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6586760301417650259L;

	private static Map<EventType, Integer> trendEventRequiredMap = new HashMap<EventType, Integer>();

	private static final String PROPERTIES_FILE_NAME = "common.properties";
	private static final String PROPERTIES_FILE_VAR = "commonproperties";
	private static String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);
	private static final String EVENTTYPE_COL_NAME = "eventtype";
	private static final String EVENTREQUIRED_COL_NAME = "required";
	private static CassandraContext csContext;

	//public TrendRecoFilter() throws IOException {
	static {
		if(propertiesFilePath == null)
			propertiesFilePath = SparkFiles.get(PROPERTIES_FILE_NAME);

		String nodeIpList = null;
		String dataKeyspace = null;

		PropertiesHandler phandler = null;
		try {
			csContext = new CassandraContext(null);
			phandler = new PropertiesHandler(propertiesFilePath);
			nodeIpList = phandler.getValue(CommonPropKeys.cs_hostList);	
			dataKeyspace = phandler.getValue(CommonPropKeys.cs_fisKeyspace);
			csContext.connect(nodeIpList);
			ResultSet rs = csContext.getRows(dataKeyspace, "eo_event_metadata");
			List<Row> rows = rs.all();
			for (Row row : rows) {
				trendEventRequiredMap.put(EventType.valueOf(row.getString(EVENTTYPE_COL_NAME)),row.getInt(EVENTREQUIRED_COL_NAME));
			}
		} catch (IOException e) {
			//throw e;
			e.printStackTrace();
		} finally {
			//if(phandler!=null)
			//phandler.close();
			csContext.close();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <TEntity extends SimpleFoundationEntity> TEntity doFilter(TEntity e)
			throws FilterException {
		UserEvent ue = (UserEvent) e;
		if(trendEventRequiredMap.containsKey(ue.eventType)) {
			ue.eventrequired = trendEventRequiredMap.get(ue.eventType);
		} else {
			ue.eventrequired = 0;
		}
		return (TEntity) ue;
	}
}