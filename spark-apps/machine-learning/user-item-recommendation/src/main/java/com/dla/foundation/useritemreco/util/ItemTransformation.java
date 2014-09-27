package com.dla.foundation.useritemreco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.useritemreco.model.UserItemRecoCF;

/**
 * This class is used to transform record of item in cassandra format into
 * required format
 * 
 * @author shishir_shivhare
 * 
 */
public class ItemTransformation implements Serializable {

	private static final long serialVersionUID = -6776591049279416170L;
	private static final String DELIMITER_PROPERTY = "#";
	private static Logger logger = Logger.getLogger(ItemTransformation.class);

	public static JavaPairRDD<String, String> getItem(

	JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		JavaRDD<String> primaryKeyRDD = cassandraRDD
				.flatMap(new FlatMapFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String>() {

					private static final long serialVersionUID = -2012039165099395701L;

					@Override
					public Iterable<String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record) {

						List<UUID> regionList = new ArrayList<>();
						List<String> regionListString;
						String tenantId = null;

						String itemId = null;
						String primaryKey = null;
						List<String> primaryKeyList = new ArrayList<>();
						Map<String, ByteBuffer> primaryKeyColumns = record._1();
						Map<String, ByteBuffer> otherColumns = record._2;

						if (null != otherColumns
								.get(UserItemRecoProp.ITEM_LEVEL_TENANT_ID)
								&& null != otherColumns
										.get(UserItemRecoProp.ITEM_LEVEL_REGION_ID)
								&& null != primaryKeyColumns
										.get(UserItemRecoCF.ID.getColumn())) {
							itemId = UUIDType.instance.compose(
									primaryKeyColumns.get(UserItemRecoCF.ID
											.getColumn())).toString();
							tenantId = UUIDType.instance
									.compose(
											otherColumns
													.get(UserItemRecoProp.ITEM_LEVEL_TENANT_ID))
									.toString();
							regionList = ListType
									.getInstance(UUIDType.instance).compose(
											otherColumns
													.get(UserItemRecoCF.REGION
															.getColumn()));
							regionListString = new ArrayList<>();
							for (UUID uuid : regionList) {
								regionListString.add(uuid.toString());
							}
							for (String regionIdEach : regionListString) {
								primaryKey = tenantId + DELIMITER_PROPERTY
										+ regionIdEach + DELIMITER_PROPERTY
										+ itemId;
								primaryKeyList.add(primaryKey);
							}
						}

						return primaryKeyList;
					}
				});

		JavaPairRDD<String, String> itemRDD = primaryKeyRDD
				.mapToPair(new PairFunction<String, String, String>() {

					private static final long serialVersionUID = 2634910667738521130L;

					@Override
					public Tuple2<String, String> call(String record) {
						String[] keys = record.split("#");
						String itemId = keys[2];
						logger.debug("Getting all details from ScoreSummary tenantId :"
								+ keys[0]
								+ " regionId :"
								+ keys[1]
								+ " itemId :" + itemId);
						return new Tuple2<String, String>(record, itemId);
					}
				});

		return itemRDD;
	}

}
