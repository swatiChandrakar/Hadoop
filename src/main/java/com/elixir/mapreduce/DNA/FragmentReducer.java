package com.elixir.mapreduce.DNA;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FragmentReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {
	  private IntWritable result = new IntWritable();
	 // private Text result1 = new Text();
	  public void reduce(Text key, Iterable<IntWritable> values, 
	                     Context context
	                     ) throws IOException, InterruptedException {
		  System.out.println("in reduce");
		  int sum = 0;
	    for (IntWritable val : values) {
	     //System.out.println("Val :-" + val.toString());		    	
	     //sum += Integer.parseInt(val.toString());	
	      //context.write(key, new IntWritable(Integer.parseInt(val.toString())));
	    	System.out.println("$$$$$$$"+val);
	    	System.out.println("$$$$$$$KEY####"+key.toString());
	    	context.write(key,new IntWritable(1));
	    }
	    
	   result.set(sum);
	 //  context.write(key, result);
	  //x context.write(key, result);
	    
	  }
	}