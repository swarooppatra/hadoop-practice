package i.practice.hadoop.maxtemperature;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class MaxTemperatureMapperTest {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();

    Text value = new Text("0043012650999991949032418004+62300+010750FM-12+048599999V0202701N00461220001CN0500001N9+00781+99999999999");
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

    try {
      mapper.map(null, value, output, null);
      
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      verify(output).collect(any(Text.class), any(IntWritable.class));
    } catch (IOException e) {
      
      e.printStackTrace();
    }
  }
}
