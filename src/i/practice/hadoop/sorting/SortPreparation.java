package i.practice.hadoop.sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import common.newapi.JobBuilder;
import common.newapi.NcdcRecordParser;

public class SortPreparation extends Configured implements Tool {

	static class SortPreparationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			parser.parse(value);
			if (parser.isValidTemperature()) {
				context.write(new IntWritable(parser.getAirTemperature()), value);
			}
		}

	}

	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		job.setMapperClass(SortPreparationMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setCompressOutput(job, true);
		//SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		//SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new SortPreparation(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
