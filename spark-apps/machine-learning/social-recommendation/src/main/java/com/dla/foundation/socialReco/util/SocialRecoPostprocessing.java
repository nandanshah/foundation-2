package com.dla.foundation.socialReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.datastax.driver.core.utils.UUIDs;
import com.dla.foundation.socialReco.model.Social;
import com.dla.foundation.socialReco.model.SocialScore;

public class SocialRecoPostprocessing implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Integer EVENTREQUIRED = 1;

	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> processingForSocialScore(
			JavaRDD<SocialScore> records) {
		
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = records
				.mapToPair(new PairFunction<SocialScore, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					Map<String, ByteBuffer> primaryKey;
					List<ByteBuffer> otherColumns;
					TimestampType timestampType;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							SocialScore records) throws Exception {
						
						timestampType = TimestampType.instance;
						primaryKey = new LinkedHashMap<String, ByteBuffer>();
						otherColumns = new ArrayList<ByteBuffer>();
						
						primaryKey.put(Social.ID.getColumn(), ByteBufferUtil
								.bytes(UUIDs.startOf(SocialRecommendationUtil
										.getFormattedDate(records
												.getTimestamp()))));
						primaryKey.put(Social.TENANT.getColumn(),
								UUIDType.instance.fromString(records
										.getTenantId()));
						primaryKey.put(Social.REGION.getColumn(),
								UUIDType.instance.fromString(records
										.getRegionId()));
						primaryKey.put(Social.ITEM.getColumn(),
								UUIDType.instance.fromString(records
										.getItemId()));
						primaryKey.put(Social.USER.getColumn(),
								UUIDType.instance.fromString(records
										.getProfileId()));
						
						otherColumns.add(ByteBufferUtil.bytes(records
								.getsocialScore()));
						otherColumns.add(ByteBufferUtil.bytes(records.getReason()));
						otherColumns.add(timestampType.decompose(new Date(
								records.getTimestamp()))); 
						otherColumns.add(ByteBufferUtil.bytes(EVENTREQUIRED)); 
						
						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								primaryKey, otherColumns);
					}
					
				});
		return cassandraOutputRDD;
	}

}
