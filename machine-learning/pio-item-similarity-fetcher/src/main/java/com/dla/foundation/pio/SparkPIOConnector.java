package com.dla.foundation.pio;

import io.prediction.Client;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.pio.util.CassandraConfig;

public class SparkPIOConnector implements Serializable {

	private static final long serialVersionUID = 8073585091774712586L;
	private static final String CURRENT_TIME = "now";
	private static Logger logger = Logger.getLogger(SparkPIOConnector.class
			.getName());

	/**
	 * Instantiates and return JavaSparkContext.
	 * 
	 * @param sparkMaster
	 *            Name of Master for JavaSparkContext.
	 * @param sparkAppName
	 *            Name of AppName for JavaSparkContext.
	 * @return Instance of JavaSparkContext.
	 */
	public JavaSparkContext initilizeSparkContext(String sparkMaster,
			String sparkAppName) {

		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster(sparkMaster);
		sparkConf.setAppName(sparkAppName);

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		return sparkContext;

	}

	/**
	 * Get Item Similarities for provided list of items from PIO with
	 * SparkContext.
	 * 
	 * @param allItemsRDD
	 *            list of items for whose item-similarity is to be fetched.
	 * @param strAppKey
	 *            AppKey that Client will use to communicate with API.
	 * @param strAppURL
	 *            AppURL that Client will use to communicate with API
	 * @param strEngine
	 *            EngineName that Client will use.
	 * @param intNumRecPerItem
	 *            Maximum number of similar items to be fetch per items.
	 * @return JavaPairRDD<String, List<String>> Key-Value pair where Key is
	 *         itemid and value is List of similarities.
	 */

	public JavaPairRDD<String, List<String>> getItemSimilarityFromPIO(
			JavaRDD<String> allItemsRDD, final String strAppKey,
			final String strAppURL, final String strEngine,
			final int intNumRecPerItem) {
		final JavaPairRDD<String, List<String>> similarItems = allItemsRDD
				.mapToPair(new PairFunction<String, String, List<String>>() {

					private static final long serialVersionUID = 5528816445669906910L;

					@SuppressWarnings("finally")
					@Override
					public Tuple2<String, List<String>> call(String itemId)
							throws Exception {
						String similarItems[] = {};
						Client client = null;
						try {
							// Currently PIO Client SDK is instantiated inside
							// call method because Client class does not
							// implements the Serilizable interface.
							client = new Client(strAppKey, strAppURL);
							similarItems = client.getItemSimTopN(
									strEngine, itemId, intNumRecPerItem);
							logger.info("Fetching similar items for Item id "
									+ itemId);
							// gets intNumRec similar items from PIO.

						} catch (IOException e) {
							logger.warn(e.getMessage());
							// If PIO doesn't find any similar item for particular
							// itemid, it throws IOException, in such cases we are
							// returning empty array for same item.

						} finally {
							client.close();

							return new Tuple2<String, List<String>>(itemId,
									Arrays.asList(similarItems));
						}

					}

				});
		return similarItems;

	}

	/**
	 * This method will filter out empty similarity list and Returns list of
	 * item that have similar items returned from PIO. If PIO doesn't find similar items for
	 * particular items it throws IOException, in such cases we are returning
	 * empty array for same item. In this method we are removing those items for
	 * whom we have provided empty list of similar items.
	 * 
	 * 
	 * @param allItemSimilarities
	 *            All similar items fetched from PIO, including empty list of
	 *            similarities.
	 * @return JavaPairRDD<String, List<String>> Key-Value pair where Key is
	 *         itemid and value is List of non-empty similarities.
	 */

	public JavaPairRDD<String, List<String>> removeEmptySimilarityRecords(
			JavaPairRDD<String, List<String>> allItemSimilarities) {
		JavaPairRDD<String, List<String>> filteredSimilarityRDD = allItemSimilarities
				.filter(new Function<Tuple2<String, List<String>>, Boolean>() {

					private static final long serialVersionUID = -7711009473196986893L;

					@Override
					public Boolean call(
							Tuple2<String, List<String>> similarityList)
							throws Exception {
						return !similarityList._2().isEmpty();
					}
				});
		return filteredSimilarityRDD;
	}

	/**
	 * Returns JavaPairRDD in (<Map<String,
	 * ByteBuffer>,java.util.List<ByteBuffer>>) format that which is used for
	 * writing data to Cassandra.
	 * 
	 * @param filteredItemSimilarities
	 *            JavaPairRDD of items those have non-empty item-similarity list.
	 * @param itemsKey
	 *            ColuumnName of the PrimaryKey of Target Cassandra Table.
	 * @return JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>>
	 *         JavaPairRDD in special format that is suitable for writing data
	 *         to Cassandra.
	 */

	public JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>> toCassandraRDDforSavingDataToCassandraTable(
			JavaPairRDD<String, List<String>> filteredItemSimilarities,
			final String itemsKey) {

		JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>> cassandraRDD = filteredItemSimilarities
				.mapToPair(new PairFunction<Tuple2<String, List<String>>, Map<String, ByteBuffer>, java.util.List<ByteBuffer>>() {

					private static final long serialVersionUID = 8894826755888085258L;
					Map<String, ByteBuffer> keys;
					List<ByteBuffer> values;

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<String, List<String>> tupleItemSimilarity)
							throws Exception {
						keys = new LinkedHashMap<String, ByteBuffer>();
						values = new ArrayList<ByteBuffer>();

						// This will convert itemid string to UUID format and
						// will put to keys MAP.
						keys.put(itemsKey,
								UUIDType.instance.fromString(tupleItemSimilarity._1));

						ArrayList<UUID> newUUIDList = new ArrayList<UUID>();
						// Converting all movieids from String type to UUID.
						for (String str : tupleItemSimilarity._2) {

							newUUIDList.add(UUIDType.instance
									.compose(UUIDType.instance.fromString(str)));
						}
						ListType list = ListType.getInstance(UUIDType.instance);
						values.add(list.decompose(newUUIDList));

						// This will add current timestamp.
						values.add(TimestampType.instance
								.fromString(CURRENT_TIME));
						return new Tuple2<Map<String, ByteBuffer>, java.util.List<ByteBuffer>>(
								keys, values);
					}

				});

		return cassandraRDD;
	}
	
	/**
	 * This method reads itemids from records read from source cassandra table.
	 *
	 * @param allItemsRecords
	 * 			RDD containing records read from Source Cassandra table
	 * 			
	 * @param cassandraConfig
	 * 			Instance of CassandraConfig
	 * 
	 * @return
	 * 		JavaRDD containing itemids in String format.
	 */

	public JavaRDD<String> getItemsFromCassandraRecords(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> allItemsRecords,
			final CassandraConfig cassandraConfig) {
		JavaRDD<String> items = allItemsRecords
				.map(new Function<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String>() {

					private static final long serialVersionUID = -1641738530261115057L;

					@Override
					public String call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> dataFromCassandra)
							throws Exception {
						Map<String, ByteBuffer> keySet = dataFromCassandra._1();
						ByteBuffer ID = keySet
								.get(cassandraConfig.sourcePrimaryKey);
						return UUIDType.instance.compose(ID).toString();
					}

				});
		return items;
	}
}
