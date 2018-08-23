package org.alan.unigram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import java.io.IOException;




public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        if (args.length < 2) {
            System.out.printf("Usage: <input dir> <output dir>\n");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", "\t");
        Job job=Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(UniMapper.class); //replace with correct mapper
        job.setReducerClass(UniReducer.class); //replace with correct reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}