package dev.mapreduce;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationTest {
	Configuration conf = null;

	@Before
	public void setup() {
		conf = new Configuration();
		conf.addResource(new Path("/Users/swaroop/git/hadoop-practice/config/test-config-1.xml"));
		conf.addResource(new Path("config/test-config-2.xml"));
	}

	@After
	public void cleanUp() {
		conf = null;
	}

	@Test
	public void testConfig() {
		assertThat(conf.get("color"), is("yellow"));
		assertThat(conf.getInt("size", 0), not(10));
		assertThat(conf.get("weight"), is("heavy"));		
		assertThat(conf.get("size-weight"), is("12,heavy"));
		System.setProperty("size", "16");
		assertThat(conf.get("size-weight"), not("15,heavy"));
	}

}
