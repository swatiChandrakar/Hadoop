package com.elixir.mapreduce.DNA;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends
		Partitioner<CompositeKey, NullWritable> {

	@Override
	public int getPartition(CompositeKey key, NullWritable value,
			int numPartitions) {
		return key.chrNo % numPartitions;
	}
}
