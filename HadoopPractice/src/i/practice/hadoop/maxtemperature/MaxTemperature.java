package i.practice.hadoop.maxtemperature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class MaxTemperature {

  public static void main(String args[]) throws IOException {
    if (args.length != 2) {
      System.err.println("USAGE: MaxTemperature <input> <output>");
      System.exit(-1);
    }

    JobConf conf = new JobConf(MaxTemperature.class);
    conf.setJobName("Max Temparature");
    conf.setProfileEnabled(true);
    conf.setProfileParams("-agentlib:hprof=cpu=sample,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
    conf.setProfileTaskRange(true, "0-2");

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    JobClient.runJob(conf);
  }

}
