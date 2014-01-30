package joining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmployeeJoinFunction extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(),
				new EmployeeJoinFunction(), args);
		System.exit(ret);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: hadoop jar EmployeeJoinFunction <emp_ip_file> <dept_ip_file> <op_dir>");
			System.exit(1);
		}
		try {
			Configuration conf = this.getConf();

			Job job = Job.getInstance(conf);
			job.setJarByClass(EmployeeJoinFunction.class);
			job.setJobName("Employee dept join");
			MultipleInputs.addInputPath(job, new Path(args[0].trim()),
					TextInputFormat.class, EmpMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1].trim()),
					TextInputFormat.class, DeptMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2].trim()));

			job.setReducerClass(EmpJoinReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			return job.waitForCompletion(true) ? 0 : 1;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return 1;
	}
}
