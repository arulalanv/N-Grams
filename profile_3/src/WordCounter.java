package org.alan.wc.profile3.Sorting;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;

public class WordCounter extends MapReduceBase implements Reducer<CompositeKey,IntWritable,CompositeKey,IntWritable> {

	public void reduce(CompositeKey key, Iterator<IntWritable> values,
			OutputCollector<CompositeKey, IntWritable> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		int sum=0;
		while(values.hasNext()) {
			IntWritable item = values.next();
			sum += item.get();
			
		}
		
		key.setCount(sum);
		output.collect(key, new IntWritable(sum));
	}

}
