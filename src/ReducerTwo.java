package tp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerTwo extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int cantidadVentas = 0;
		String sueldoBasico = "";
		String cadena = "";
		String bonus = "1";

		for (Object val : values) {

			String[] campos = val.toString().split("\t");

			if(campos.length == 3){
				cantidadVentas = Integer.parseInt(campos[1]);
				if(cantidadVentas > 500){
							bonus = "2";
				}else if(cantidadVentas <= 500 && cantidadVentas >= 251){
							bonus = "1.6";
				}else if(cantidadVentas <= 250 && cantidadVentas >= 51){
							bonus = "1.3";
				}else if(cantidadVentas < 50 && cantidadVentas >= 11){
							bonus = "1.1";
				}else if(cantidadVentas < 10 && cantidadVentas >= 1){
							bonus = "1.02";
				}
				/*for(int i=0; i<campos.length;i++){
						cadena = cadena  + "\t" +campos[i];
				}*/				
				cadena = val  + "\t" + bonus;
			}else{
				sueldoBasico =  campos[0];
			}

		}

		if(cadena.length() == 0){
			cadena = "0" + "\t" + "0"  + "\t" + "0"  + "\t" + "1";
		}

		context.write(key, new Text(sueldoBasico + "\t" + cadena));
	}

}
