package streamingapps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import com.google.common.base.Optional;

public class StreamingWC {
	public static void main(String[] args) throws Exception {
		//create context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("EjemploStreaming").setMaster("local[2]"));
		JavaStreamingContext streamContext = new JavaStreamingContext(sc, Durations.seconds(15));

		//connect stream
		JavaDStream<String> lines = streamContext.socketTextStream("localhost", 7777);
		
		//wordcount
		JavaDStream<String> words = lines.flatMap(
			new FlatMapFunction<String, String>() {
				public Iterable<String> call(String x) { 
					return Arrays.asList(x.split(" ")); 
				}
			});
			
		JavaPairDStream<String, Integer> result = words.mapToPair(
			new PairFunction<String, String, Integer>() {
				public Tuple2<String, Integer> call(String x) { 
					return new Tuple2(x, 1); 
				}
			}).reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		result.print();
		
		streamContext.start();
		streamContext.awaitTermination();
	}
}

