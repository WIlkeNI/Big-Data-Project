package tp;

import java.io.IOException;
import multipleInput.Join;

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

public class joberOne extends Configured implements Tool {
	String baseDir;

	private Job setupJobOne(String[] args) throws IOException{
		Configuration conf = getConf();

		Job job = new Job(conf, "joberOne");

	    job.setJarByClass(multiInputFile.class);
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WCMapperJob1.class)
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, WCMapperJob1.class)

	    //configure Mapper
	    job.setMapperClass(WCMapperJob1.class);
	    job.setMapOutputKeyClass(Text.class);
	    // job.setMapOutputValueClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);

	    //configure Reducer
	    job.setReducerClass(WCReducerJob1.class);

			job.setNumReduceTasks(1);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    //job.setNumReduceTasks(10);
	    job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

	    FileSystem fs = FileSystem.get(conf);
	    String inputDirOne = args[0];
	    String inputDirTwo = args[1];
	    String outputDir = "salidaJobOne";
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir),true);
	    }

            //job.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job, new Path(inputDirOne));
	    FileInputFormat.addInputPath(job, new Path(inputDirTwo));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;
	}

	private Job setupJobTwo() throws IOException{

	    Configuration conf = getConf();

		Job job = new Job(conf, "joberTwo");

	    job.setJarByClass(Worker.class);

	    //configure Mapper
	    job.setMapperClass(WCMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);

	    //configure Reducer
	    job.setReducerClass(WCReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);

	    //job.setNumReduceTasks(10);
	    job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

	    FileSystem fs = FileSystem.get(conf);
	    String inputDirOne = "salidaJobOne";
	    String outputDir = "salidaJobTwo";
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir),true);
	    }

            //job.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job, new Path(inputDirOne));
	    FileInputFormat.addInputPath(job, new Path(inputDirTwo));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));

	    return job;
	}

	private Job setupJobTree() throws IOException{

	    return job;
	}

	private Job setupJobFour() throws IOException{

	    return job;
	}

	private Job setupJobFive() throws IOException{

	    return job;
	}

	private Job setupJobSix() throws IOException{

	    return job;
	}

	private Job setupJobSeven() throws IOException{

	    return job;
	}

	@Override
	public int run(String[] args) throws Exception {
	    Job job;
	    boolean success;
		//ArrayWritable topFive;
		//ArrayWritable tablaBonusPersonal;
		//ArrayWritable tablaBonusDepartamento;
		Configuration conf = getConf();

		//se ejecuta el job 1
	    job = setupJobOne(args);
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }

		//se ejecuta el job 2
	    job = setupJobTwo();
		//Aca se debe escribir en el contexto el top 5 de departamentos

		//conf.setArray("topFive", topFive );
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }

		//se ejecuta el job 3
	    job = setupJobTree();
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }

		//se ejecuta el job 4
	    job = setupJobFour();
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }

		//se ejecuta el job 5
	    job = setupJobFive();
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }

		//se ejecuta el job 6
	    job = setupJobSix();
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }

		//se ejecuta el job 7
	    job = setupJobSeven();
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }

	    return 0;
	}


}
