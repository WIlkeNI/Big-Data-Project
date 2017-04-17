package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WCReducer extends Reducer<Text, LongWritable, Text, Text/*LongWritable*//*Deberia devolver un text con las columnas concatenadas*/> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String str_salida;
		// int times = 0;
		for (@SuppressWarnings("unused") Object val : values) {
			// times++;
			str_salida = str_salida + ' ' + val;
		}

		// context.write(key, new LongWritable(times));
		context.write(key, new Text(str_salida));
	}

}
