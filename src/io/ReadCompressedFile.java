package io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

public class ReadCompressedFile {

  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(new Path(uri));
    Path file = new Path(uri);
    Path outFile = new Path(args[1]);
    FileSystem fs;
    FSDataInputStream in = null;
    try {
      fs = FileSystem.get(URI.create(uri), conf);
      in = fs.open(file);
      CompressionInputStream compIn = codec.createInputStream(in);
      FSDataOutputStream out = fs.create(outFile);
      IOUtils.copyBytes(compIn, out, conf, true);
    } finally {
      IOUtils.closeStream(in);
    }

  }

}
