package dev.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureDriver extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2) {
			System.err.println("Invalid use of program. Usage : hadoop dev.mapreduce.MaxTemperatureDriver [generic option] <input> <output>");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = new Job(getConf(), "Max Temperature");
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
		System.exit(exitCode);
	}

}
