package com.elixir.mapreduce.DNA;

import java.io.IOException;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.InputSplit;  
import org.apache.hadoop.mapreduce.JobContext;  
import org.apache.hadoop.mapreduce.RecordReader;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.io.LongWritable;

public class WholeFileInputFormat extends FileInputFormat<Text, IntWritable>{  
  @Override  
  protected boolean isSplitable(JobContext context, Path file){  
    System.out.println("set isplitable");  
    return false;  
  }  
  @Override  
  public RecordReader<Text, IntWritable> createRecordReader  
  (InputSplit split,TaskAttemptContext context) throws IOException,  
  InterruptedException {  
	  LineRecordReader reader = new LineRecordReader();  
    reader.initialize(split,context);  
    return reader;  
  }  

}  
