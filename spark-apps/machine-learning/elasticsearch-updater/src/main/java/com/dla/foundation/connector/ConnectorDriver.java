package com.dla.foundation.connector;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.data.cassandra.CassandraEntityReader;
import com.dla.foundation.connector.persistence.elasticsearch.DeleteESType;
import com.dla.foundation.connector.persistence.elasticsearch.ESWriter;
import com.google.protobuf_spark.TextFormat.ParseException;
/*
 * Driver class for Cassandra To ES application.
 * Mainly deals with reading reco type from ES and deleting the passive reco type.
 * Finally it inserts records into passive reco type.
 * 
 */

public class ConnectorDriver 
{
	private static Logger logger = Logger.getLogger(ConnectorDriver.class);
	/*
	 * @param : 2 arguments - common properties file and application resources location
	 * 						  Resources location required to map the schema in ES and this path contains different json mappings.
	 */
    public static void main(String[] args) throws java.text.ParseException
    {
      if(args.length==2){
    	 ConnectorDriver driver= new ConnectorDriver();
    	 driver.run(args);
      }
      else
    	  System.err.println("USAGE: ConnectorDriver takes 2 arguments:common propertis file, common schema path ");
    }
    
    /*
     * run method gives calls to 3 tasks listed below:
     * 			1. Read the active/passive reco type from ES (As it stored in ES at <es_ip>:<es:port>/catalog/reco_type/_show.)
     * 			2. Delete all the docs from passive reco type.
     * 			3. Insert the records read from cassandra into ES.
     */
    public void run(String[] args) throws java.text.ParseException{
    	CassandraEntityReader userReco= new CassandraEntityReader();
    	DeleteESType deleteTypes= new DeleteESType();
    	
		ESWriter.init(args[0],args[1]);
		logger.info("Deleting passive user reco i.e. "+ESWriter.reco_type.getPassive()+" from catalog index");
		deleteTypes.deleteType(args[0]);

		logger.info("Satrting Connector- Reading records from Cassandra and writing to ES");
			try {
				userReco.runUserRecoDriver(args[0]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
 }
