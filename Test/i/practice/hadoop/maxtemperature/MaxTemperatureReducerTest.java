package i.practice.hadoop.maxtemperature;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class MaxTemperatureReducerTest {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    MaxTemperatureReducer reducer = new MaxTemperatureReducer();

    Text key = new Text("1991");
    Iterator<IntWritable> values = Arrays.asList(new IntWritable(10),
        new IntWritable(45), new IntWritable(43)).iterator();

    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

    reducer.reduce(key, values, output, null);

    verify(output).collect(new Text("1991"), new IntWritable(45));

  }

}
