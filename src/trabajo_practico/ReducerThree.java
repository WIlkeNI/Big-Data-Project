package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerThree extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		float importe = 0;
		int ventas = 0;

		for (@SuppressWarnings("unused") Object val : values) {
			String[] campos = val.toString().split("\t");
			importe += Float.parseFloat(campos[1]);
			ventas += Integer.parseInt(campos[0]);
		}
		//falta darle presicion al importe porque omite los decimales
		context.write(key, new Text(ventas + "\t" + String.format("%f",importe)));
	}

}
