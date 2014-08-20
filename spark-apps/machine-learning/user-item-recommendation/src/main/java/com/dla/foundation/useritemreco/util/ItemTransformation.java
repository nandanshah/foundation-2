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


/**
 * This class is used to transform record of item in cassandra format into
 * required format
 * 
 * @author shishir_shivhare
 * 
 */
public class ItemTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6776591049279416170L;
	private static final String DELIMITER_PROPERTY = "#";

	public static JavaPairRDD<String, String> getItem(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,final Map<String,String> regionTenantInfo) {

		JavaPairRDD<String, String> itemRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -5603807529160303375L;

					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
									throws Exception {
						String tenantId = null;
						String regionId = null;
						String itemId = null;
						String primaryKey = null;
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {

								if (column
										.getKey()
										.compareToIgnoreCase(
												regionTenantInfo.get(PropKeys.TENANT_ID.getValue())) == 0) {
									if (null != column.getValue())
										tenantId = UUIDType.instance.compose(
												column.getValue()).toString();

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												regionTenantInfo.get(PropKeys.REGION_ID.getValue())) == 0) {
									if (null != column.getValue())
										regionId = UUIDType.instance.compose(
												column.getValue()).toString();

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.ID.getColumn()) == 0) {
									if (null != column.getValue())
										itemId = UUIDType.instance.compose(
												column.getValue()).toString();

								}

							}
						}
						if (null != tenantId && null != regionId) {
							primaryKey = tenantId + DELIMITER_PROPERTY
									+ regionId + DELIMITER_PROPERTY + itemId;
							return new Tuple2<String, String>(primaryKey,
									itemId);
						}
						return null;

					}
				});

		return itemRDD;
	}

}
