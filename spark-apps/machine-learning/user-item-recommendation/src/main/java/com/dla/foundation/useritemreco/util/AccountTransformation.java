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
 * This class is used to transform record of account in cassandra format into
 * required format
 * 
 * @author shishir_shivhare
 * 
 */
public class AccountTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6776591049279416170L;

	public static JavaPairRDD<String, String> getAccount(
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
						String tenantId = null;
						String accountId = null;
						String primaryKey = null;
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {

								if (column.getKey().compareToIgnoreCase(
										userItemRecoCF.ID.getColumn()) == 0) {
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
								if (column.getKey().compareToIgnoreCase(
										userItemRecoCF.TENANT.getColumn()) == 0) {
									if (null != column.getValue())
										tenantId = UUIDType.instance.compose(
												column.getValue()).toString();

								}
							}
						}
						if (null != accountId && null != tenantId) {
							primaryKey = accountId;
							return new Tuple2<String, String>(primaryKey,
									tenantId);
						}
						return null;

					}
				});

		return profileRDD;
	}

}
