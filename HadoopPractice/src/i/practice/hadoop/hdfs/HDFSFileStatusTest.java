package i.practice.hadoop.hdfs;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSFileStatusTest {

  private MiniDFSCluster miniCluster;

  FileSystem fs;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    miniCluster = new MiniDFSCluster(conf, 1, true, null);
    fs = FileSystem.get(conf);
    OutputStream out = fs.create(new Path("/data/file"));
    out.write("content".getBytes("UTF-8"));
    out.close();
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
  }

  @Test(expected = FileNotFoundException.class)
  public void throwFileNotFound() throws IOException {
    fs.getFileStatus(new Path("no-such-file"));
  }

  @Test
  public void testFile() throws IOException {
    Path file = new Path("/data/file");
    FileStatus status = fs.getFileStatus(file);
    assertThat(status.getBlockSize(), is(64 * 1024 * 1024L));
  }

}
