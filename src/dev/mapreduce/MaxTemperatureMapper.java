package dev.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		String temperature = line.substring(87, 92);
		if (!isMissing(temperature)) {
			context.write(new Text(year), new IntWritable(Integer.parseInt(temperature)));
		}
	}

	private boolean isMissing(String temp) {
		return temp.equals("+9999");
	}

}
