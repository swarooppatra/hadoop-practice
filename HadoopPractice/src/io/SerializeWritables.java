package io;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class SerializeWritables {

  public static void main(String[] args) throws Exception {

  }

  public static byte[] serialize(Writable writable) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    writable.write(dataOut);
    dataOut.close();
    return out.toByteArray();
  }

  public static void deSerialize(Writable writable, byte[] data)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(data);
    DataInputStream dataIn = new DataInputStream(in);
    writable.readFields(dataIn);
    dataIn.close();
    in.close();
  }

  @Test
  public void testIntSerialization() throws Exception {
    IntWritable writable = new IntWritable(123);
    byte[] data = serialize(writable);
    System.out.println(StringUtils.byteToHexString(data));
    assertThat(data.length, is(4));
    IntWritable newWritable = new IntWritable();
    deSerialize(newWritable, data);
    assertThat(newWritable.get(), is(writable.get()));
  }

}
