package joining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmpSkillMain extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Invalid usage. Usage : EmpSkillMain <emp_file> <output_dir>");
			System.exit(1);
		}
		Job job = Job.getInstance(getConf());
		job.setJarByClass(EmpSkillMain.class);
		job.setJobName("Employees with same skills");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(EmpSkillMapper.class);
		job.setReducerClass(EmpSkillReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EmpSkillMain(), args);
		System.exit(res);
	}

}
