package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;


public class ReducerSix extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Double subBasico;
		Double subBonusEmpleado;
		Double subBonusDepartamento;
		TreeMap<Double, Integer> tmap = new TreeMap<Double, Integer>(Collections.reverseOrder());

		for (@SuppressWarnings("unused") Object val : values) {
			String[] i = val.toString().split("\t");
			subBasico = Double.parseDouble(i[1]);
			subBonusEmpleado = (Double.parseDouble(i[1]) * Double.parseDouble(i[2])) - Double.parseDouble(i[1]);
			subBonusDepartamento = (Double.parseDouble(i[1]) * Double.parseDouble(i[3])) - Double.parseDouble(i[1]);
			Double sueldoTotal = subBasico + subBonusEmpleado + subBonusDepartamento;
      context.write(new LongWritable(Integer.parseInt(i[0])), new Text(String.valueOf(sueldoTotal)));
			tmap.put(sueldoTotal, Integer.parseInt(i[0]));
		}

		context.write(new LongWritable(0), new Text("  "));

	  Set set = tmap.entrySet();
  	Iterator iterator = set.iterator();
		int topTen = 1;
    while(iterator.hasNext()) {
       Map.Entry mentry = (Map.Entry)iterator.next();
      //  System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
      //  System.out.println(mentry.getValue());
			 if (topTen < 11){
					context.write(new LongWritable(topTen), new Text(String.valueOf(mentry.getValue()) + "\t" + String.valueOf(mentry.getKey())));
					topTen ++;
			 }
    }


	}

}
