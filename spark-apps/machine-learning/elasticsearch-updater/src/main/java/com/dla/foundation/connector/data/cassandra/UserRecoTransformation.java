package com.dla.foundation.connector.data.cassandra;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.connector.model.ESEntity;
import com.dla.foundation.connector.model.UserRecoSummary;
import com.dla.foundation.connector.model.UserRecommendation;
import com.dla.foundation.connector.persistence.elasticsearch.ESWriter;

public class UserRecoTransformation implements Serializable, CassandraESTransformer {

	/**
	 * 
	 */
	private static final long serialVersionUID = 138414123858979446L;
	private static ESWriter es_writer= new ESWriter();
	
	
	@Override
	public  JavaPairRDD<String, ESEntity> extractEntity(JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		
		JavaPairRDD<String, ESEntity> userEventRDD = cassandraRDD.
				mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, ESEntity>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -7902017743866489489L;
					UserRecommendation userReco = null;

					public Tuple2<String, ESEntity> call(Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)	throws Exception {
						String primaryKey = "";
						userReco = getUserReco(record);
						es_writer.writeToES(userReco.getTenantId(), userReco.getUserid(), userReco.getParentItemId(), userReco);
						if (null != userReco) {
								if(null!=userReco.getUserid()){
								primaryKey = primaryKey.concat("");
								if (0 != "".compareTo(primaryKey)) {
	
									return new Tuple2<String, ESEntity>(
											primaryKey, userReco);
								}
							}

						}
						return null;
					}
				});

		return userEventRDD;
	}
	private static UserRecommendation getUserReco(
			Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
			throws NumberFormatException, CharacterCodingException, ParseException {

		UserRecommendation userReco = new UserRecommendation();
		Map<String, ByteBuffer> priamryKeyColumns = record._1();
		if (priamryKeyColumns != null) {
			for (Entry<String, ByteBuffer> column : priamryKeyColumns.entrySet()) {
				if (column.getKey().toLowerCase().compareTo(UserRecoSummary.USER.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setUserid(UUIDType.instance.compose(column.getValue()).toString());

				} else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.ITEM.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setParentItemId(UUIDType.instance.compose(column.getValue()).toString());
				} else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.TENANT.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setTenantId(UUIDType.instance.compose(column.getValue()).toString());
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.REGION.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setRegionId(UUIDType.instance.compose(column.getValue()).toString());

				}
			
			}
		}
		Map<String, ByteBuffer> otherColumns = record._2;
		if (otherColumns != null) {

			for (Entry<String, ByteBuffer> column : otherColumns.entrySet()) {
				if (column.getKey().toLowerCase().compareTo(UserRecoSummary.TRENDSCORE.getColumn()) == 0) {
					if (null != column.getValue()){
						userReco.setTrendScore(ByteBufferUtil.toDouble(column.getValue()));
					}
				} else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.POPULARITYSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setPopularScore(ByteBufferUtil.toDouble((column.getValue())));

				} else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.SOCIALSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setSocialScore(ByteBufferUtil.toDouble((column.getValue())));
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.FPSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setFpScore(ByteBufferUtil.toDouble((column.getValue())));
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.NEWSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setNewScore(ByteBufferUtil.toDouble((column.getValue())));
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.DATE.getColumn()) == 0) {
					if (null != column.getValue()){
						userReco.setDate(TimestampType.instance.compose(column.getValue()));
								
					}
				}
				

			}
		}
		return userReco;
	}

}