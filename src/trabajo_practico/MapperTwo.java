package tp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

//									   Input key	 Input val	  Output key	   Output val
public class MapperTwo extends Mapper<LongWritable,   Text,        LongWritable, 	   Text/*LongWritable*/>{ //devolver un text concatenando cada uno de los campos?

    private static LongWritable one = new LongWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			//El key del resultado del Mapper seria id_Empleado
			//el resto de los valores hay que concatenarlo en un string
			try{
				String[] result = value.toString().split("\t");
				int depto = Integer.parseInt(result[1]);
				String cant_ventas = result[2];
				String importe_ventas = result[3];
				
				context.write(new LongWritable(depto), new Text(cant_ventas + "\t" + importe_ventas));
			}catch(Exception e){
				//...
			}
			Configuration conf = context.getConfiguration(); 
			conf.setInt("esto", 8);
	}



}
