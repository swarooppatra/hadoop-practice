package dev.mapreduce;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
	private final int MISSSING_TEMP = 9999;

	private String year;
	private int temperature;
	private String quality;

	public void parse(String record) {
		String tempString;
		year = record.substring(15, 19);
		if (record.charAt(87) == '+') {
			tempString = record.substring(88, 92);
		} else {
			tempString = record.substring(87, 92);
		}
		temperature = Integer.parseInt(tempString);
		quality = record.substring(92, 93);
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	public boolean isValidTemperature() {
		return temperature != MISSSING_TEMP && quality.matches("[01459]");
	}

	public int getTemperature() {
		return temperature;
	}

	public String getYear() {
		return year;
	}

}
