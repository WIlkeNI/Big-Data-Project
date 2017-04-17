package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//									   Input key	 Input val	  Output key	   Output val
public class WCMapperJob1 extends Mapper<LongWritable,   Text,        Text, 	   Text/*LongWritable*/>{ //devolver un text concatenando cada uno de los campos?

	// private static LongWritable one = new LongWritable(1);
	String str_salida = '';

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		// Text word = new Text();
		// word.set(value.toString());
		/*String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\"().,[]/-' ;", false);
	    while(tokenizer.hasMoreTokens()){
	    	word.set(tokenizer.nextToken());
	    	context.write(word, one);
	    }*/

			//El key del resultado del Mapper seria id_Empleado
			//el resto de los valores hay que concatenarlo en un string
			String[] result = value.split("\\t");
			String key = result[0];
			while(int x=1; x <= result.lenght(); x++ ){
				str_salida = str_salida + ' ' + result[x];
			}
			context.write(key, str_salida);
		// context.write(value, one);
	}



}
