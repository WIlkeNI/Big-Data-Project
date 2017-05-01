package tp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//									   Input key	 Input val	  Output key	   Output val
public class MapperFourA extends Mapper<LongWritable,   Text,        LongWritable, 	   Text>{ //ver de cambiar la salida de Long a Integer

    // private static LongWritable one = new LongWritable(1);


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			//El key del resultado del Mapper seria id_Empleado
			//el resto de los valores hay que concatenarlo en un string
			try{
				// String[] result = value.toString().split("\t");
				// int depto = Integer.parseInt(result[1]);
				// String cant_ventas = result[2];
				// String importe_ventas = result[3];

      	context.write(key, new Text(value + "\t" + "A"));

			}catch(Exception e){
				//...
			}
	}



}
