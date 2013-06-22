package dev.mapreduce;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {

	static {
		Configuration.addDefaultResource("mapred-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		for (Entry<String, String> entry : conf) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}

}
