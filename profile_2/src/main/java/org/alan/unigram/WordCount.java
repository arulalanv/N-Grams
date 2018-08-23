package org.alan.unigram;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, MapWritable>{

		private Text word = new Text();
		private Text docId = new Text();
		MapWritable mw = new MapWritable();

    public void map(LongWritable key , Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	if(value.toString().contains("<====>")) {
    		String temp[] = value.toString().toLowerCase().split("<====>");
    		docId.set(temp[1]);
    		word.set(temp[2]);
    	}else
    	{
    		word.set(value.toString());
    	}
    	StringTokenizer tokenizer = new StringTokenizer(word.toString());
    	while(tokenizer.hasMoreTokens()) {
    		mw.put(new Text(tokenizer.nextToken().trim()), new IntWritable(1));
    		context.write(docId, mw);
    	}
    	
    	
    }
  }
  
  
  
  

  public static class IntSumReducer
       extends Reducer<Text,MapWritable,Text,Text> {

    public void reduce(Text key, Iterable<MapWritable> values,Context context) throws IOException, InterruptedException {

    		/*for(MapWritable w : values) {
    			Set<Writable> s = w.keySet();
    			Iterator<Writable> r = s.iterator();
    			while(r.hasNext()) {
    				String ss = r.next().toString().trim();
    				context.write(key,new Text(ss +" "+w.get(ss)+"\n"));
    			}
    		}*/
    	
		for(MapWritable w : values) {
			Set<Writable> s = w.keySet();
			Iterator<Writable> r = s.iterator();
			while(r.hasNext()) {
				context.write(key,new Text(r.next().toString()+" "+w.values().size()+"\n"));
			}
		}
    	    	


    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
