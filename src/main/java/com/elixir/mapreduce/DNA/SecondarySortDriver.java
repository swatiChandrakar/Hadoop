package com.elixir.mapreduce.DNA;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySortDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: hadoop jar <jar-name> <class-name> <input-paths> <output-path>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Secondary Sort Example: DNA Mapping");
		job.setJarByClass(SecondarySortDriver.class);

		job.setMapperClass(DNAMapper.class);
		// job.setCombinerClass(DNAReducer.class);
		job.setReducerClass(DNAReducer.class);

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setPartitionerClass(SecondarySortPartitioner.class);
		//job.setSortComparatorClass(CompositeKeySortComparator.class);
		job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);

		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		job.setNumReduceTasks(CompositeKey.NUMBER_OF_CHROMOSOMES);

		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
