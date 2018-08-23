package org.alan.wc.profile3.Sorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordMapper extends MapReduceBase implements Mapper<Object,Text,CompositeKey,IntWritable> {
	final private static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, OutputCollector<CompositeKey, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String str = value.toString();
		String[] values = str.split(" ");
		
		for(String item : values) {
			if(item.trim().isEmpty()) {continue;}
			output.collect(new CompositeKey(item, 1),one);
		}
		
		
	}
	
	

}
