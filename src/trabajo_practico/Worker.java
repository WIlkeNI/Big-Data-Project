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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

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
	
		Job job = new Job(conf, "joberOne");
	    
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
	}/*

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
	}*/
	
	@Override
	public int run(String[] args) throws Exception {
	    Job job;
	    boolean success;
		//ArrayWritable topFive;
		Configuration conf = getConf(); 
	    
		//se ejecuta el job 1
	    job = setupJobOne(args); 	    
	    success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }
		
		//se ejecuta el job 2
	    job = setupJobTwo(args);
		//Aca se debe escribir en el contexto el top 5 de departamentos
		
		success = job.waitForCompletion(true);
	    if (!success){
	    	System.out.println("Error job");
	    	return -1;
	    }
		/*
		//se ejecuta el job 3
	    job = setupJobTree();
	    //conf.setArray("topFive", topFive ); 	       
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
	    }*/
	    
	    return 0;
	}


}
		
