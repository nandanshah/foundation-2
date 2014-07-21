package com.dla.foundation;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.search.SearchHit;

import com.dla.foundation.elasticsearch.mlt.ESServer;
import com.dla.foundation.elasticsearch.mlt.MoreLikeThisFieldQuery;

import scala.Tuple2;

public class ESSparkConnector implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String sparkMaster;
	private String sparkAppName;
	private String sparkPort;
	private String esServerHostname;
	private String transportPort;
	private String httpPort;
	private String _index;
	private String _type;
	private String keySpace;
	private String columnFamily;
	private String primaryKey;
	private String esNodeName;

	public ESSparkConnector(String sparkMaster, String sparkAppName,
			String sparkPort, String esServerHostname, String esPort,
			String httpPort, String _index, String _type, String esNodeName,
			String keySpace, String columnFamily, String primaryKey) {
		this.sparkMaster = sparkMaster;
		this.sparkAppName = sparkAppName;
		this.sparkPort = sparkPort;
		this.esServerHostname = esServerHostname;
		this.transportPort = esPort;
		this.httpPort = httpPort;
		this._index = _index;
		this._type = _type;
		this.esNodeName = esNodeName;
		this.keySpace = keySpace;
		this.columnFamily = columnFamily;
		this.primaryKey = primaryKey;
	}

	private JavaSparkContext getSparkContext(String appName) {
		SparkConf sc = new SparkConf();
		if (this.sparkMaster.equalsIgnoreCase("local")) {
			sc.setMaster("local");
		} else
			sc.setMaster("spark://" + this.sparkMaster + ":" + this.sparkPort);
		sc.setAppName(appName);
		sc.set("spark.serializer", KryoSerializer.class.getName());
		return new JavaSparkContext(sc);
	}

	/**
	 * 
	 * @return Returns all the movies. Key - Doc Id Value - Entire JSON Doc in
	 *         MapWritable data format
	 */
	public JavaPairRDD<Text, MapWritable> getAllMovies() {
		JobConf conf = new JobConf();
		conf.setSpeculativeExecution(true);
		conf.set("es.nodes", this.esServerHostname + ":" + this.httpPort);
		conf.set("es.resource", this._index + "/" + this._type);
		JavaSparkContext jsc = getSparkContext(this.sparkAppName);
		@SuppressWarnings("unchecked")
		JavaPairRDD<Text, MapWritable> esRDD = jsc.newAPIHadoopRDD(conf,
				EsInputFormat.class, Text.class, MapWritable.class);
		return esRDD;
	}

	/**
	 * 
	 * @param allMovies
	 *            - RDD of all movies
	 * @return Return Similar Movies in Cassandra compatible format key :
	 *         Map<"Movie",movie_id> value : list[similarMovie's Id]
	 */
	public JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> getSimilarMovies(
			JavaPairRDD<Text, MapWritable> allMovies) {
		return allMovies.mapToPair(new MovieSimilarity());
	}

	/**
	 * Class for implementing Pair function. For each record, it queries ES to
	 * find 10 similar movies. These are returned in a List<ByteBuffer> format
	 * which can be written to cassandra.
	 */
	private class MovieSimilarity
			implements
			PairFunction<Tuple2<Text, MapWritable>, Map<String, ByteBuffer>, List<ByteBuffer>> {

		private static final long serialVersionUID = 1290661346141607913L;

		public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
				Tuple2<Text, MapWritable> movie) throws Exception {
			int guid = Integer
					.parseInt(movie._2.get(new Text("id")).toString());
			Client esClient = new ESServer(
					ESSparkConnector.this.esServerHostname,
					Integer.parseInt(ESSparkConnector.this.transportPort),
					ESSparkConnector.this.esNodeName).getESClient();
			SearchResponse mltResponse = new MoreLikeThisFieldQuery(
					ESSparkConnector.this._index, ESSparkConnector.this._type,
					esClient).getMoreLikeThisMovies(guid, 10);
			List<String> similarMovies = new ArrayList<String>();
			for (SearchHit s : mltResponse.getHits()) {
				similarMovies.add(s.getId());
			}
			esClient.close();
			ListType<String> listType = ListType.getInstance(UTF8Type.instance);
			List<ByteBuffer> columnsToWrite = new ArrayList<ByteBuffer>();
			columnsToWrite.add(listType.decompose(similarMovies));
			Map<String, ByteBuffer> movieEntry = new LinkedHashMap<String, ByteBuffer>();
			movieEntry.put(ESSparkConnector.this.primaryKey,
					ByteBufferUtil.bytes(String.valueOf(guid)));
			return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
					movieEntry, columnsToWrite);
		}
	}
}
