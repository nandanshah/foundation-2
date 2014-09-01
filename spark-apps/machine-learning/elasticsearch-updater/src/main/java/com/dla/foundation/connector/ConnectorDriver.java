package com.dla.foundation.connector;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.data.cassandra.CassandraEntityReader;
import com.dla.foundation.connector.persistence.elasticsearch.DeleteESType;
import com.dla.foundation.connector.persistence.elasticsearch.ESWriter;
import com.google.protobuf_spark.TextFormat.ParseException;

public class ConnectorDriver 
{
	private static Logger logger = Logger.getLogger(ConnectorDriver.class);

    public static void main(String[] args) throws java.text.ParseException
    {
      if(args.length==2){
    	 ConnectorDriver driver= new ConnectorDriver();
    	 driver.run(args);
      }
      else
    	  System.err.println("USAGE: ConnectorDriver takes 2 arguments:common propertis file, common schema path ");
    }
    
    
    public void run(String[] args) throws java.text.ParseException{
    	CassandraEntityReader userReco= new CassandraEntityReader();
    	//DeleteESType deleteTypes= new DeleteESType();
    	
		ESWriter.init(args[0],args[1]);
		//logger.info("Deleting user reco type from all indexes");
		//deleteTypes.deleteType(args[0]);

		logger.info("Satrting Connector- Reading records from Cassandra and writing to ES");
			try {
				userReco.runUserRecoDriver(args[0]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
 }
