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
import com.dla.foundation.socialReco.model.FriendsInfo;

public class FriendsInfoTransformation  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * This method reads data from social_friends_info table. 
	 * Convert it from byteBuffer(format provided through spark-cassandra connector) 
	 * format to readable format.
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param friendsInfoCassandraProp
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 */
	public static JavaPairRDD<String, String> readFriendsInfoData(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,CassandraConfig friendsInfoCassandraProp
			) throws IOException, ParseException {
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf= new Configuration();

		cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
				friendsInfoCassandraProp.getInputKeyspace(),
				friendsInfoCassandraProp.getInputColumnfamily(),
				friendsInfoCassandraProp.getPageRowSize());
	
		JavaPairRDD<String, String> friendsInfo = dataTransformation(cassandraRDD);
		return friendsInfo;
	}

	/**
	 * This class provides the utility to convert the record (format provided
	 * through spark-cassandra connector) to JavaPairRDD<String, String>.
	 * 
	 * */
	
	public static JavaPairRDD<String, String> dataTransformation(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD)	{
		JavaPairRDD<String, String> frndsInfo = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>,
						String, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
									throws Exception {
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						String profileId = null;
						String friendId =null;

						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {
								if (column.getKey().toLowerCase()
										.compareTo(FriendsInfo.profileid.getValue()) == 0) {
									if (null != column.getValue())
										profileId = UUIDType.instance.compose(column
												.getValue()).toString();

								} else if (column.getKey().toLowerCase()
										.compareTo(FriendsInfo.friendid.getValue()) == 0) {
									if (null != column.getValue())
										friendId = UUIDType.instance.compose(column
												.getValue()).toString();

								} 
							}
						}
						return new Tuple2<String, String>(friendId, profileId);
					}
				});
		return frndsInfo;
	}

}
