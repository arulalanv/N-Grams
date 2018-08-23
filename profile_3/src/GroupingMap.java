package org.alan.wc.profile3.Sorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GroupingMap extends MapReduceBase implements Mapper<Object,Text,CompositeKey,IntWritable> {

	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void map(Object arg0, Text item, OutputCollector<CompositeKey, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String str = item.toString();
		String[] items = str.split("\t");
		CompositeKey ck = new CompositeKey(items[0],Integer.parseInt(items[items.length-1]));
		output.collect(ck, new IntWritable(ck.getCount()));
	}

}
