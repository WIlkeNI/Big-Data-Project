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

    private static LongWritable one = new LongWritable(1);


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			try{


      	context.write(one, new Text(value));

			}catch(Exception e){
				//...
			}
	}



}
