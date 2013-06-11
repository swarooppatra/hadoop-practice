package i.practice.hadoop.mapreduce;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigTest {
  Configuration conf = null;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.addResource(new Path("config\\test-config-1.xml"));
    conf.addResource(new Path("config\\test-config-2.xml"));
  }

  @After
  public void tearDown() throws Exception {
    conf = null;
  }

  @Test
  public void testConfiguration() {
    assertThat(conf.get("Name", "none"), is("none"));
    assertThat(conf.get("color", "none"), is("yellow"));
    assertThat(conf.getInt("size", 0), is(12));
    assertThat(conf.get("weight", ""), is("heavy"));
    assertThat(conf.get("MyName", "Swaroop"), is("Swaroop"));
    assertThat(conf.get("size-weight"), is("12,heavy"));
    System.setProperty("size", "14");
    assertThat(conf.get("size-weight"), is("14,heavy"));
    // Overrides final variables too
    System.setProperty("weight", "ligt");
    assertThat(conf.get("size-weight"), is("14,ligt"));
    System.setProperty("tool", "hadoop");
    assertThat(conf.get("tool"), is((String) null));
    
  }

}
