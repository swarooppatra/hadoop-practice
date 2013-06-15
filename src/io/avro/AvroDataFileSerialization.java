package io.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class AvroDataFileSerialization {

	public static void main(String[] args) throws IOException {
		Parser parser = new Parser();
		Schema schema = parser.parse(new File("config/stringpair.avsc"));

		GenericRecord record = new GenericData.Record(schema);
		record.put("left", "L");
		record.put("right", "R");

		File dataFile = new File("stringpair.avro");
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		dataFileWriter.create(schema, dataFile);
		dataFileWriter.append(record);
		dataFileWriter.close();

		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(dataFile, reader);
		if (dataFileReader.hasNext()) {
			GenericRecord result = dataFileReader.next();
			System.out.println(result.get("left"));
			System.out.println(result.get("right"));
		}
	}

}
