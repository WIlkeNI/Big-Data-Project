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

		for (Object val : values) {
			String[] i = val.toString().split("\t");
			float sueldo_basico = Float.parseFloat(i[1]);
			float bonus_personal = Float.parseFloat(i[2]);
			float bonus_depto = Float.parseFloat(i[3]);

			subBasico = subBasico + sueldo_basico;
			subBonusEmpleado += (sueldo_basico * bonus_personal) - sueldo_basico;
			subBonusDepartamento += (sueldo_basico * bonus_depto) - sueldo_basico;
		}

		float total = subBasico + subBonusEmpleado + subBonusDepartamento;

		context.write(key, new Text(String.format("%f",subBasico) + "\t" + String.format("%f",subBonusEmpleado) + "\t" + String.format("%f",subBonusDepartamento) + "\t" + String.format("%f",total)));
	}

}
