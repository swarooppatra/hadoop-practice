package i.practice.hadoop.maxtemperature;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureReducer extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text key, Iterator<IntWritable> value,
      OutputCollector<Text, IntWritable> output, Reporter repoter)
      throws IOException {
    int max = Integer.MIN_VALUE;
    while (value.hasNext()) {
      max = Math.max(max, value.next().get());
    }
    output.collect(key, new IntWritable(max));
  }

}
