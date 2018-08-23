package org.alan.wc.profile3.Sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobItem  extends Configured implements Tool {
	
	public Job parseInputAndOutput(Path in,Path out) throws IOException{
		JobConf conf = new JobConf(JobItem.class);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)) {
			fs.delete(out,true);
		}
		
		FileInputFormat.setInputPaths(conf,in);
		FileOutputFormat.setOutputPath(conf,out);
		
		conf.setOutputKeyClass(CompositeKey.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(WordMapper.class);
		conf.setCombinerClass(WordCounter.class);
		conf.setReducerClass(WordCounter.class);
		
		Job job = Job.getInstance(conf,"WordCount");
		
		job.setGroupingComparatorClass(CompositeKeySorter.class);
		
		return job;
		
	}
	
	public Job group(Path in,Path out) throws IOException{
		JobConf conf = new JobConf(JobItem.class);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)) {
			fs.delete(out,true);
		}
		FileInputFormat.setInputPaths(conf,in);
		FileOutputFormat.setOutputPath(conf,out);
		
		conf.setJobName("Descending Occurance");
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(CompositeKey.class);
		conf.setMapperClass(GroupingMap.class);
		Job job = Job.getInstance(conf,"Grouper");
		
		job.setSortComparatorClass(WordCountSorter.class);
		job.setGroupingComparatorClass(CompositeKeySorter.class);
		
		return job;
		
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		int exitCode = ToolRunner.run(new JobItem(), args);

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length < 2) {
			System.err.println("please type the input and output files");
			System.exit(0);
		}
		
		Job j = parseInputAndOutput(new Path(args[0]),new Path(args[0]+ ".tmp"));
		System.out.println("Before Submission");
		j.waitForCompletion(false);
		System.out.println("Submitted!!!");
		j=group(new Path(args[0]+".tmp"),new Path(args[1]));
		return j.waitForCompletion(false)? 0:1;
	}

}
