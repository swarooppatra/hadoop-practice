package io.filedatabase;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadTest {

  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    Path file = new Path(uri);
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),
        conf);
    Writable value = (Writable) ReflectionUtils.newInstance(
        reader.getValueClass(), conf);
    long position = reader.getPosition();
    while (reader.next(key, value)) {
      String syncSeen = reader.syncSeen() ? "*" : "";
      System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
      position = reader.getPosition();
    }
    System.out.println("\nSeek test at record boundry\n");
    reader.seek(359);
    position = reader.getPosition();
    reader.next(key, value);
    System.out.println("[" + position + "]" + "\t" + key.toString() + "\t"
        + value.toString());

    System.out.println("\nSeek test at non record boundry\n");
    try {
      reader.seek(361);
      position = reader.getPosition();
      reader.next(key, value);
      System.out.println("[" + position + "]" + "\t" + key.toString() + "\t"
          + value.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.println("\nSync test at non record boundry\n");
    reader.sync(362);
    position = reader.getPosition();
    reader.next(key, value);
    System.out.println("[" + position + "]" + "\t" + key.toString() + "\t"
        + value.toString());

    IOUtils.closeStream(reader);
  }

}
