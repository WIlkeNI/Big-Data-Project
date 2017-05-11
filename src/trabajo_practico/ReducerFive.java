package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerFive extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		float subBasico = 0;
		float subBonusEmpleado = 0;
		float subBonusDepartamento = 0;

		for (@SuppressWarnings("unused") Object val : values) {
			String[] i = val.toString().split("\t");
			subBasico = subBasico + Float.parseFloat(i[1]);
			subBonusEmpleado += (Float.parseFloat(i[1]) * Float.parseFloat(i[2])) - Float.parseFloat(i[1]);
			subBonusDepartamento += (Float.parseFloat(i[1]) * Float.parseFloat(i[3])) - Float.parseFloat(i[1]);
		}

		float total = subBasico + subBonusEmpleado + subBonusDepartamento;

		context.write(key, new Text(String.valueOf(subBasico) + "\t" + String.valueOf(subBonusEmpleado) + "\t" + String.valueOf(subBonusDepartamento) + "\t" + String.valueOf(total)));
	}

}
