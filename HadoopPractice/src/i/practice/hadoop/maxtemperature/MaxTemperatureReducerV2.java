package i.practice.hadoop.maxtemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducerV2 extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  protected void reduce(Text key, Iterable<IntWritable> value, Context context)
      throws IOException, InterruptedException {
    int max = Integer.MIN_VALUE;
    for (IntWritable temp : value) {
      max = Math.max(max, temp.get());
    }
    context.write(key, new IntWritable(max));
  }

}
