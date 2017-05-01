package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerFour extends Reducer<LongWritable, Text, LongWritable, Text/*LongWritable*//*Deberia devolver un text con las columnas concatenadas*/> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Text valEmit = new Text();
		String merge = "";
		int i =0;


    HashMap<Integer, Double> bonusDepartamento = new HashMap<Integer, Double>();
    bonusDepartamento.put(1, 1.35);   bonusDepartamento.put(2, 1.3);
    bonusDepartamento.put(3, 1.25);   bonusDepartamento.put(4, 1.2);
    bonusDepartamento.put(4, 1.15);   bonusDepartamento.put(6, 1.1);

    for (@SuppressWarnings("unused") Object val : values) {
			// String[] campos = val.toString().split("\t");
			// importe += Double.parseDouble(campos[1]);
			// ventas += Integer.parseInt(campos[0]);

			if(i == 0){
						merge = val.toString() + "\t";
					}
					else{
						merge += val.toString();
					}

					i++;

		}
			valEmit.set(merge);
			context.write(key, valEmit);
		// context.write(key, new Text(ventas + "\t" + String.format("%f",importe)));
	}

}
