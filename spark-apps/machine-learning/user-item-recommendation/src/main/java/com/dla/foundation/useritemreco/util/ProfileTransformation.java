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
 * This class is used to transform record of profile in cassandra format into
 * required format
 * 
 * @author shishir_shivhare
 * 
 */
public class ProfileTransformation implements Serializable {

	private static final long serialVersionUID = -6776591049279416170L;
	private static final String DELIMITER = "#";
	private static Logger logger = Logger
			.getLogger(ProfileTransformation.class);

	public static JavaPairRDD<String, String> getProfile(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		JavaPairRDD<String, String> profileRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					private static final long serialVersionUID = -5603807529160303375L;

					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record) {
						String regionId = null;
						String profileId = null;
						String accountId = null;
						String primaryKey = null;
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						Map<String, ByteBuffer> otherColumns = record._2();

						if (null != priamryKeyColumns.get(UserItemRecoCF.ID
								.getColumn())
								&& null != priamryKeyColumns
										.get(UserItemRecoCF.ACCOUNT.getColumn())
								&& null != otherColumns
										.get(UserItemRecoProp.PROFILE_LEVEL_PREFERRED_REGION_ID)) {
							profileId = UUIDType.instance.compose(
									priamryKeyColumns.get(UserItemRecoCF.ID
											.getColumn())).toString();
							accountId = UUIDType.instance.compose(
									priamryKeyColumns
											.get(UserItemRecoCF.ACCOUNT
													.getColumn())).toString();
							regionId = UUIDType.instance
									.compose(
											otherColumns
													.get(UserItemRecoProp.PROFILE_LEVEL_PREFERRED_REGION_ID))
									.toString();
						}

						if (null != profileId && null != regionId) {
							primaryKey = accountId;
							logger.debug("Reading profile table regionId :"
									+ regionId + " profileId :" + profileId);
							return new Tuple2<String, String>(primaryKey,
									regionId + DELIMITER + profileId);
						}
						return null;

					}
				});

		return profileRDD;
	}

}
