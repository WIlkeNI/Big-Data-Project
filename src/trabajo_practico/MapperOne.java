package tp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//									   Input key	 Input val	  Output key	   Output val
public class MapperOne extends Mapper<LongWritable,   Text,        LongWritable, 	   Text/*LongWritable*/>{ //devolver un text concatenando cada uno de los campos?

    private static LongWritable one = new LongWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			//El key del resultado del Mapper seria id_Empleado
			//el resto de los valores hay que concatenarlo en un string
			try{
				String[] result = value.toString().split("\t");
				int k = Integer.parseInt(result[0]);
				context.write(new LongWritable(k), new Text(result[1]+ "\t" +result[2]+ "\t" +result[3]));
			}catch(Exception e){
				//...
			}
	}



}
