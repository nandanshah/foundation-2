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
import com.dla.foundation.connector.model.UserRecommendation;
import com.dla.foundation.connector.persistence.elasticsearch.ESWriter;
import com.dla.foundation.connector.util.UserRecoSummary;

/*
 * This class converts the Cassandra records to a form needed by ES. Create a object with 
 * all column values and send it to ES
 * 
 * @author neha_jain
 */
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
						es_writer.writeToES(userReco.getTenantId(), userReco.getprofileId(), userReco.getmediaItemId(), userReco);
						if (null != userReco) {
								if(null!=userReco.getprofileId()){
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
		userReco.id = null;
		Map<String, ByteBuffer> priamryKeyColumns = record._1();
		if (priamryKeyColumns != null) {
			for (Entry<String, ByteBuffer> column : priamryKeyColumns.entrySet()) {
				if (column.getKey().toLowerCase().compareTo(UserRecoSummary.USER.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setprofileId(UUIDType.instance.compose(column.getValue()).toString());
						
				} else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.ITEM.getColumn()) == 0) {
					if (null != column.getValue())
						userReco.setmediaItemId(UUIDType.instance.compose(column.getValue()).toString());
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
		userReco.id = userReco.getmediaItemId()+ "-" +userReco.getprofileId()  ;
		Map<String, ByteBuffer> otherColumns = record._2;
		if (otherColumns != null) {

			for (Entry<String, ByteBuffer> column : otherColumns.entrySet()) {
				userReco.setEnabled(1);
				if (column.getKey().toLowerCase().compareTo(UserRecoSummary.TRENDSCORE.getColumn()) == 0) {
					if (null != column.getValue()){
						if (ByteBufferUtil.toDouble(column.getValue()) < 0)
							userReco.setTrendScore(0);
						else
							userReco.setTrendScore(ByteBufferUtil.toDouble(column.getValue()));
					}
				} else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.POPULARITYSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						if (ByteBufferUtil.toDouble(column.getValue()) < 0)
							userReco.setPopularScore(0);
						else
							userReco.setPopularScore(ByteBufferUtil.toDouble((column.getValue())));
						
				} else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.SOCIALSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						if (ByteBufferUtil.toDouble(column.getValue()) < 0)
							userReco.setSocialScore(0);
						else
							userReco.setSocialScore(ByteBufferUtil.toDouble((column.getValue())));
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.FPSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						if (ByteBufferUtil.toDouble(column.getValue()) < 0)
							userReco.setFpScore(0);
						else
							userReco.setFpScore(ByteBufferUtil.toDouble((column.getValue())));
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.NEWSCORE.getColumn()) == 0) {
					if (null != column.getValue())
						if (ByteBufferUtil.toDouble(column.getValue()) < 0)
							userReco.setNewScore(0);
						else
							userReco.setNewScore(ByteBufferUtil.toDouble((column.getValue())));
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.DATE.getColumn()) == 0) {
					if (null != column.getValue()){
						userReco.setDate(TimestampType.instance.compose(column.getValue()));
						
						/*SimpleDateFormat formatter = new SimpleDateFormat("EEEE, dd/MM/yyyy/hh:mm:ss");
						String dateInString= new SimpleDateFormat("MM/dd/yyyy").format(TimestampType.instance.compose(column.getValue()));
						Date parsedDate = formatter.parse(dateInString);		
				*/	}
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.TRENDREASON.getColumn()) == 0) {
					if (null != column.getValue()){
						userReco.setTrendreason(ByteBufferUtil.string(column.getValue()));
								
					}
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.FPREASON.getColumn()) == 0) {
					if (null != column.getValue()){
						userReco.setFpreason(ByteBufferUtil.string(column.getValue()));
								
					}
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.SOCIALREASON.getColumn()) == 0) {
					if (null != column.getValue()){
						userReco.setSocialreason(ByteBufferUtil.string(column.getValue()));
								
					}
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.RECOBYFOUNDATIONREASON.getColumn()) == 0) {
					if (null != column.getValue()){
						System.out.println("reco col value"+ByteBufferUtil.string(column.getValue()));
						userReco.setRecoByfoundationreason(ByteBufferUtil.string(column.getValue()));
								
					}
				}
				else if (column.getKey().toLowerCase().compareTo(UserRecoSummary.POULARITYREASON.getColumn()) == 0) {
					if (null != column.getValue()){
						userReco.setPopularityreason(ByteBufferUtil.string(column.getValue()));
								
					}
				}
			}
		}
		return userReco;
	}

}
