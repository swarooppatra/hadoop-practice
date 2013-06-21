package io.filedatabase;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteTest {

  private static final String[] DATA = { "One, two, buckle my shoe",
      "Three, four, shut the door", "Five, six, pick up sticks",
      "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    Path file = new Path(uri);
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file,
        IntWritable.class, Text.class);
    IntWritable key = new IntWritable();
    Text value = new Text();

    for (int i = 0; i < 100; i++) {
      key.set(i);
      value.set(DATA[i % DATA.length]);
      System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
      writer.append(key, value);
    }
    IOUtils.closeStream(writer);
  }

}
