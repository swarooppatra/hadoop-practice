package joining;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpSkillMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		String empName = tokens[1].trim();
		String[] skills = tokens[3].trim().split(",");
		for (String skill : skills) {
			System.out.println(skill + "\t" + empName);
			context.write(new Text(skill.trim()), new Text(empName));
		}
	}

}
