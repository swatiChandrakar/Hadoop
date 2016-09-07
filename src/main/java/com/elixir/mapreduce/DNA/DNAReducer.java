package com.elixir.mapreduce.DNA;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public  class DNAReducer 
     extends Reducer<CompositeKey,NullWritable,CompositeKey, NullWritable> {

	public void reduce(CompositeKey key, Iterable<NullWritable> values, 
                     Context context
                     ) throws IOException, InterruptedException {
    for (NullWritable val : values) {
    	context.write(key, NullWritable.get());
    }
  }
}
