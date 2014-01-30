package joining;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpSkillReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String empNames = "";
		for (Text emp : values) {
			empNames = empNames + emp.toString() + "\t";
		}
		context.write(key, new Text(empNames.trim()));
	}

}
