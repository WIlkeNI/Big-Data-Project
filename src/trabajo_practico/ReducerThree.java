package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class ReducerThree extends Reducer<LongWritable, Text, LongWritable, Text/*LongWritable*//*Deberia devolver un text con las columnas concatenadas*/> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		double importe = 0;
		int ventas = 0;
		Configuration conf = context.getConfiguration(); 
		
		for (@SuppressWarnings("unused") Object val : values) {
			String[] campos = val.toString().split("\t");
			importe += Double.parseDouble(campos[1]);
			ventas += Integer.parseInt(campos[0]);
		}
		conf.set(key.toString(), String.valueOf(importe));
		System.out.println(key.toString());
		conf.setInt("esto", 9);

		context.write(key, new Text(ventas + "\t" + String.format("%f",importe)));
	}

}
