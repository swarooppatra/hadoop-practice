package i.practice.hadoop.maxtemperature;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class MaxTemperatureDriverTest {

  @Test
  public void testRun() throws Exception {
    JobConf conf = new JobConf();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");

    Path input = new Path("input/ncdc/micro");
    Path output = new Path("max-temp");

    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(output, true);

    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    driver.setConf(conf);

    int exitCode = driver.run(new String[] { input.toString(),
        output.toString() });

    assertThat(exitCode, is(0));

  }

}
