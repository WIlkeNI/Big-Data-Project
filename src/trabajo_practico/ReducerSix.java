package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;


public class ReducerSix extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		float subBasico = 0;
		float subBonusEmpleado = 0;
		float subBonusDepartamento = 0;
		TreeMap<Long, Integer> tmap = new TreeMap<Long, Integer>(Collections.reverseOrder());

		for (@SuppressWarnings("unused") Object val : values) {
			String[] i = val.toString().split("\t");
			subBasico = Float.parseFloat(i[1]);
			subBonusEmpleado = (Float.parseFloat(i[1]) * Float.parseFloat(i[2])) - Float.parseFloat(i[1]);
			subBonusDepartamento = (Float.parseFloat(i[1]) * Float.parseFloat(i[3])) - Float.parseFloat(i[1]);
			float sueldoTotal = subBasico + subBonusEmpleado + subBonusDepartamento;
      context.write(new LongWritable(Integer.parseInt(i[0])), new Text(String.valueOf(sueldoTotal)));
			tmap.put(new Long((long)sueldoTotal), Integer.parseInt(i[0]));
		}

	  Set set = tmap.entrySet();
  	Iterator iterator = set.iterator();
		int topTen = 1;
    while(iterator.hasNext()) {
       Map.Entry mentry = (Map.Entry)iterator.next();
      //  System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
      //  System.out.println(mentry.getValue());
			 if (topTen < 10){
					context.write(new LongWritable((long)mentry.getKey()), new Text(String.valueOf(mentry.getValue())));
					topTen ++;
			 }
    }


	}

}
