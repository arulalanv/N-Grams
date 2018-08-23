package org.alan.unigram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;




import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



import java.io.IOException;



public class UniMapper extends Mapper<LongWritable,Text,Text,Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        Text t = new Text();
        String[] words = null;
        
        if(value.toString().contains("<====>")) {
       	 
       	 String[] ss = value.toString().split("<====>");
       	 		t.set(ss[2].toLowerCase());
       	 		words = t.toString().split(" ");
        }else
        {
        		words = value.toString().toLowerCase().split(" ");
        }
        
        //date = date.replace(",", " ");
        //temp = date.split("\\s+");
       // String year = temp[temp.length - 1].trim();

        //line = line.toLowerCase().replaceAll("[^A-Za-z0-9 ]", "");

        

        for(String word: words) {
            if (word.trim().length() > 0) {
                context.write(new Text(word), new Text("one"));
            }
        }
    }
}