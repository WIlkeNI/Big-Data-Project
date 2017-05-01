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
public class MapperFourB extends Mapper<LongWritable,   Text,        LongWritable, 	   Text>{ //ver de cambiar la salida de Long a Integer

    // private static LongWritable one = new LongWritable(1);


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			//El key del resultado del Mapper seria id_Empleado
			//el resto de los valores hay que concatenarlo en un string
			try{
				// String[] result = value.toString().split("\t");
				// int depto = Integer.parseInt(result[1]);
				// String cant_ventas = result[2];
				// String importe_ventas = result[3];


         String[] result = value.toString().split("\t");
         int newKey = Integer.parseInt(result[1]);
         String bonus_personal = result[4]; // es el ultimo valor de "listado_temporal"
         String sueldo_basico = result[3];
         String cant_ventas = result[2];
         String NOSEQUEES = result[0];
         String oldKey = key.toString(); //Parsear la clave vieja de Long a Text
         context.write(new LongWritable(newKey), new Text(bonus_personal + "\t" + sueldo_basico + "\t" + cant_ventas + "\t" + NOSEQUEES + "\t" + oldKey));

			}catch(Exception e){
				//...
			}
	}



}
