package tp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Worker extends Configured implements Tool {
	String baseDir;

	private Job setupJobOne(String[] args) throws IOException{
		Configuration conf = getConf();

		Job job = new Job(conf, "joberOne");

	    job.setJarByClass(Worker.class);

	    //configure Mapper
	    job.setMapperClass(MapperOne.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    //configure Reducer
	    job.setReducerClass(ReducerOne.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);

	    //job.setNumReduceTasks(10);
	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

	    FileSystem fs = FileSystem.get(conf);
	    String inputDir = args[0];
	    String outputDir = "resumen_ventas";
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir),true);
	    }

	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;
	}

	private Job setupJobTwo(String[] args) throws IOException{
	    Configuration conf = getConf();

		Job job = new Job(conf, "joberTwo");

	    job.setJarByClass(Worker.class);

	    //configure Mapper
	    job.setMapperClass(MapperOne.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    //configure Reducer
	    job.setReducerClass(ReducerTwo.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);

	    //job.setNumReduceTasks(10);
	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

	    FileSystem fs = FileSystem.get(conf);
	    String inputDir = args[1];
	    String inputDirTwo = "resumen_ventas";
	    String outputDir = "listado_temporal";
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir),true);
	    }

	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileInputFormat.addInputPath(job, new Path(inputDirTwo));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;
	}

	private Job setupJobThree(String[] args) throws IOException{
	    Configuration conf = getConf();

		Job job = new Job(conf, "joberThree");

	    job.setJarByClass(Worker.class);

	    //configure Mapper
		job.setMapperClass(MapperTwo.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    //configure Reducer
	    job.setReducerClass(ReducerThree.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);

	    //job.setNumReduceTasks(10);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileSystem fs = FileSystem.get(conf);
	    String inputDir = "resumen_ventas";
	    String outputDir = "punto_5";
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir),true);
	    }

	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;

	}

	private Job setupJobFour() throws IOException{

		Configuration conf = getConf();

		Job job = new Job(conf, "joberFour");

	    job.setJarByClass(Worker.class);

	    //configure Mapper
		job.setMapperClass(MapperFourA.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    //configure Reducer
	    job.setReducerClass(ReducerFour.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);

	    //job.setNumReduceTasks(10);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
      
	    FileSystem fs = FileSystem.get(conf);
	    String inputDir = "punto_5";
	    String inputDir2 = "listado_temporal";
	    String outputDir = "bonus_dpto";
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir),true);
	    }

		// MultipleInputs.addInputPath(job, new Path(inputDir2), TextInputFormat.class, MapperFourA.class);
		//  	MultipleInputs.addInputPath(job, new Path(inputDir1), TextInputFormat.class, MapperFourB.class);
	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileInputFormat.addInputPath(job, new Path(inputDir2));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;
	}


	private Job setupJobFive() throws IOException{

		Configuration conf = getConf();

		Job job = new Job(conf, "joberFive");

	    job.setJarByClass(Worker.class);

	    //configure Mapper
		job.setMapperClass(MapperFourA.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    //configure Reducer
	    job.setReducerClass(ReducerFive.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);

	    //job.setNumReduceTasks(10);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileSystem fs = FileSystem.get(conf);
	    String inputDir = "top5_manual";
	    String outputDir = "subTotales";
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir),true);
	    }

	    FileInputFormat.addInputPath(job, new Path(inputDir));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;
	}


	private Job setupJobSix() throws IOException{

			Configuration conf = getConf();

			Job job = new Job(conf, "joberSix");

		    job.setJarByClass(Worker.class);

		    //configure Mapper
			job.setMapperClass(MapperFourA.class);
		    job.setMapOutputKeyClass(LongWritable.class);
		    job.setMapOutputValueClass(Text.class);

		    //configure Reducer
		    job.setReducerClass(ReducerSix.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Text.class);

		    //job.setNumReduceTasks(10);
		    job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

		    FileSystem fs = FileSystem.get(conf);
		    String inputDir = "top5_manual";
		    String outputDir = "sueldos";
		    if(fs.exists(new Path(outputDir))){
		       fs.delete(new Path(outputDir),true);
		    }

		    FileInputFormat.addInputPath(job, new Path(inputDir));
		    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;
	}

	@Override
	public int run(String[] args) throws Exception {
	    Job job;
	    boolean success;
		//ArrayWritable topFive;
		Configuration conf = getConf();

		// se ejecuta el job 1
		/*job = setupJobOne(args);
		success = job.waitForCompletion(true);
		if (!success){
			System.out.println("Error job");
			return -1;
	  	}*/
		//
		//se ejecuta el job 2
		/*job = setupJobTwo(args);
		//Aca se debe escribir en el contexto el top 5 de departamentos	
		success = job.waitForCompletion(true);
		if (!success){
			System.out.println("Error job");
			return -1;
	  	}*/
		//se ejecuta el job 3
	    /*job = setupJobThree(args);
	    //conf.setArray("topFive", topFive );
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }*/

		//se ejecuta el job 4
	    job = setupJobFour();
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
		}

		//se ejecuta el job 5
	    // job = setupJobFive();
	    // success = job.waitForCompletion(true);
	    // if (!success){
	    // 	System.out.println("Error job");
	    // 	return -1;
	    // }

		//se ejecuta el job 6
	    // job = setupJobSix();
	    // success = job.waitForCompletion(true);
	    // if (!success){
	    // 	System.out.println("Error job");
	    // 	return -1;
	    // }

	    return 0;
	}


}
