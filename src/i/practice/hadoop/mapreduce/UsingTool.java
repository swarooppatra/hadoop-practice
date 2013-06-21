package i.practice.hadoop.mapreduce;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UsingTool extends Configured implements Tool {

  static {
    Configuration.addDefaultResource("core-site.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  public int run(String[] arg0) throws Exception {
    Configuration conf = getConf();
    for (Entry<String, String> entry : conf) {
      System.out.printf("%-40s=%s\n", entry.getKey(), entry.getValue());
    }
    return 0;
  }

  public static void main(String[] args) {
    int exitCode = 0;
    try {
      exitCode = ToolRunner.run(new UsingTool(), args);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }

}