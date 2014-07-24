package com.dla.foundation.useritemreco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.useritemreco.model.userItemRecoCF;

public class ProfileTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6776591049279416170L;
	private static final String DELIMITER = "#";

	public static JavaPairRDD<String, String> getProfile(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {

		JavaPairRDD<String, String> profileRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -5603807529160303375L;

					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
							throws Exception {
						String regionId = null;
						String profileId = null;
						String accountId = null;
						String primaryKey = null;
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {

								if (column
										.getKey()
										.toLowerCase()
										.compareTo(
												userItemRecoCF.ID.getColumn()) == 0) {
									if (null != column.getValue())
										profileId = UUIDType.instance.compose(
												column.getValue()).toString();

								} else if (column
										.getKey()
										.toLowerCase()
										.compareTo(
												userItemRecoCF.ACCOUNT
														.getColumn()) == 0) {
									if (null != column.getValue())
										accountId = UUIDType.instance.compose(
												column.getValue()).toString();

								}

							}

						}
						Map<String, ByteBuffer> otherColumns = record._2;
						if (otherColumns != null) {

							for (Entry<String, ByteBuffer> column : otherColumns
									.entrySet()) {
								if (column
										.getKey()
										.toLowerCase()
										.compareTo(
												userItemRecoCF.PREFERRED_REGION
														.getColumn()) == 0) {
									if (null != column.getValue())
										regionId = UUIDType.instance.compose(
												column.getValue()).toString();

								}
							}
						}

						if (null != profileId && null != regionId) {
							primaryKey = accountId;
							return new Tuple2<String, String>(primaryKey,
									profileId + DELIMITER + regionId);
						}
						return null;

					}
				});

		return profileRDD;
	}

}
