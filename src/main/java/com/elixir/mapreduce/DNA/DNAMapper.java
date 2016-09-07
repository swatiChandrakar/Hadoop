package com.elixir.mapreduce.DNA;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class DNAMapper 
extends Mapper<Object, Text, CompositeKey, NullWritable>{

	public void map(Object key, Text value, Context context
	             ) throws IOException,	 InterruptedException {
		
		context.write(new CompositeKey(value), NullWritable.get());
	}
}
