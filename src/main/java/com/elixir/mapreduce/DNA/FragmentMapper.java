package com.elixir.mapreduce.DNA;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FragmentMapper extends Mapper<Text, Text, Text, Text>{

@Override
public void run(Context context) throws IOException, InterruptedException {
setup(context);
while (context.nextKeyValue()) {
map(context.getCurrentKey(), context.getCurrentValue(), context);
}
cleanup(context);
}

public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {

	String []temp2=(value.toString().split(","));

System.out.println("Mapper input" + value.toString());
System.out.println("Mapper split" + String.valueOf(temp2.length));

context.write((Text) key, new Text(String.valueOf(temp2.length)));

}
}
