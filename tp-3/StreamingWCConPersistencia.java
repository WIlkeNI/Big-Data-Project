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

public class StreamingWCConPersistencia {
	public static void main(String[] args) throws Exception {
		int secs = Integer.parseInt(args[0]);
		
		//create context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("EjemploStreaming").setMaster("local[2]"));
		JavaStreamingContext streamContext = new JavaStreamingContext(sc, Durations.seconds(secs));

		//connect stream
		JavaDStream<String> lines = streamContext.socketTextStream("localhost", 7777);
		
		//checkpoint
		streamContext.checkpoint("tmp");
		
		//Se eliminan los registros repetidos (mismo idUsuario y idProducto)
		lines = lines.transform(new Function<JavaRDD<String>, JavaRDD<String>>(){
			public JavaRDD<String> call(JavaRDD<String> rows) throws Exception {
				return rows.distinct();
			}		
		});

		//Se realiza el split eliminando el idUsuario
		JavaDStream<String> words = lines.flatMap(
			new FlatMapFunction<String, String>() {
				public Iterable<String> call(String x) {
					String[] split = x.split("\t");
					return Arrays.asList(result[1]); 
				}
			});
	
		//Se crean las tuplas agrupando por idProducto
		JavaPairDStream<String, Integer> result = lines.mapToPair(
			new PairFunction<String, String, Integer>() {
				public Tuple2<String, Integer> call(String idProducto) { 
					return new Tuple2(idProducto, 1); 
				}
			});

		//Se contabilizan los productos visitados y se persiste para poder mantener la información en las siguientes ventanas temporales
		result = result.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
		    public Optional<Integer> call(List<Integer> values, Optional<Integer> current) throws Exception {
		        if (values == null || values.isEmpty()) {
		            return current;
		        }
		        int sum = current.or(0);
		        for (Integer v : values) {
		            sum += v;
		        }
		        return Optional.of(sum);
		    }
		});
		
		result.print();
		
		streamContext.start();
		streamContext.awaitTermination();
	}
}

