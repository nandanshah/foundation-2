package com.dla.foundation.useritemreco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.useritemreco.model.UserItemRecoCF;

/**
 * This class is used to transform record of account in cassandra format into
 * required format
 * 
 * @author shishir_shivhare
 * 
 */
public class AccountTransformation implements Serializable {

	private static final long serialVersionUID = -6776591049279416170L;
	private static final Logger logger = Logger
			.getLogger(AccountTransformation.class);

	public static JavaPairRDD<String, String> getAccount(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		JavaPairRDD<String, String> profileRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					private static final long serialVersionUID = -5603807529160303375L;

					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record) {
						String tenantId = null;
						String accountId = null;
						String primaryKey = null;
						Map<String, ByteBuffer> primaryKeyColumns = record._1();
						Map<String, ByteBuffer> otherColumns = record._2();

						if (null != primaryKeyColumns.get(UserItemRecoCF.ID
								.getColumn())
								&& null != otherColumns
										.get(UserItemRecoCF.TENANT_ID
												.getColumn())) {
							accountId = UUIDType.instance.compose(
									primaryKeyColumns.get(UserItemRecoCF.ID
											.getColumn())).toString();
							tenantId = UUIDType.instance.compose(
									otherColumns.get(UserItemRecoCF.TENANT_ID
											.getColumn())).toString();
						}

						if (null != accountId && null != tenantId) {
							primaryKey = accountId;
							logger.debug("Transforming records from account table accountId :"
									+ accountId);
							return new Tuple2<String, String>(primaryKey,
									tenantId);
						}
						return null;

					}
				});

		return profileRDD;
	}

}
