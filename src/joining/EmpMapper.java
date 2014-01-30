package joining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	final String prefix = "emp:";

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] tokens = line.split("\t");
		String empName = tokens[1].trim();
		int deptId = Integer.parseInt(tokens[2].trim());
		context.write(new IntWritable(deptId), new Text(prefix + empName));

	}

}
