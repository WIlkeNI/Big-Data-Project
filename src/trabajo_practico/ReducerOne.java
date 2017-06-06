package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerOne extends Reducer<LongWritable, Text, LongWritable, Text/*LongWritable*//*Deberia devolver un text con las columnas concatenadas*/> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		float importe = 0;
		int ventas = 0;
		String depto = "";

		for (@SuppressWarnings("unused") Object val : values) {
			String[] i = val.toString().split("\t");
			importe = importe + Float.parseFloat(i[2]);
			ventas = ventas + Integer.parseInt(i[1]);
			depto = i[0];
		}

		context.write(key, new Text(depto + "\t" + ventas + "\t" + importe));
	}

}
