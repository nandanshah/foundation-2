package com.dla.foundation.socialReco.util;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.socialReco.model.CassandraConfig;
import com.dla.foundation.socialReco.model.ProfileData;

public class ProfileTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * This method reads data from profile table. 
	 * Convert it from byteBuffer(format provided through spark-cassandra connector) 
	 * format to readable format.
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param profileCassandraProp
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 */
	public static JavaPairRDD<String, String>  readProfileData(JavaSparkContext sparkContext, 
			CassandraSparkConnector cassandraSparkConnector,CassandraConfig profileCassandraProp) throws IOException, ParseException {

		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf= new Configuration();

		cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
				profileCassandraProp.getInputKeyspace(),
				profileCassandraProp.getInputColumnfamily(),
				profileCassandraProp.getPageRowSize());
		
		JavaPairRDD<String, String> profileInfo = profileTransformation(cassandraRDD);

		return profileInfo;
	}
	
	/**
	 * This class provides the utility to convert the record (format provided
	 * through spark-cassandra connector) to JavaPairRDD<String, String>.
	 * 
	 * */
	public static JavaPairRDD<String, String> profileTransformation(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD)	{
		JavaPairRDD<String, String> profileData = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					private static final long serialVersionUID = 1L;
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
									throws Exception {
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						String profileId = null;
						String homeRegionId =null;

						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {

								if (column.getKey().toLowerCase()
										.compareTo(ProfileData.profileid.getValue()) == 0) {
									if (null != column.getValue())
										profileId = UUIDType.instance.compose(column
												.getValue()).toString();
								}
							}
						}
						Map<String, ByteBuffer> otherColumns = record._2;
						if (otherColumns != null) {

							for (Entry<String, ByteBuffer> column : otherColumns.entrySet()) {
								if (column.getKey().toLowerCase()
										.compareTo(ProfileData.homeregionid.getValue()) == 0) {
									if (null != column.getValue())
										homeRegionId = UUIDType.instance.compose(column
												.getValue()).toString();

								} 
							}

						}

						return new Tuple2<String, String>(profileId,homeRegionId);
					}
				});
		return profileData;
	}
}
