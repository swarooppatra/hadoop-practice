package i.practice.hadoop.maxtemperature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureV2 {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length != 2) {
      System.err.println("USAGE: MaxTemperature <input> <output>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(MaxTemperatureV2.class);
    job.setJobName("Max Temperature V2");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MaxTemperatureMapperV2.class);
    job.setCombinerClass(MaxTemperatureReducerV2.class);
    job.setReducerClass(MaxTemperatureReducerV2.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}
