package joining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpJoinReducer extends Reducer<IntWritable, Text, Text, Text> {

	@Override
	protected void reduce(IntWritable deptId, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		String dept = "";
		StringBuffer employees = new StringBuffer();
		for (Text value : values) {
			String val = value.toString();
			if (val.startsWith("emp:")) {
				employees.append(val.substring(4));
				employees.append(",");
			} else if (val.startsWith("dept:")) {
				dept = val.substring(5);
			} else {
				System.out.println("Invalid value " + val);
			}
		}
		employees.deleteCharAt(employees.lastIndexOf(","));
		context.write(new Text(dept), new Text(employees.toString()));
	}

}
