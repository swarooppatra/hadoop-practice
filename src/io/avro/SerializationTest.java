package io.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

public class SerializationTest {

	public static void main(String[] args) throws IOException {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(new File("config/stringpair.avsc"));
		
		GenericRecord record = new GenericData.Record(schema);
		record.put("left", "L");
		record.put("right", "R");
		
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> dataOut = new GenericDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(bOut, null);
		dataOut.write(record, encoder);
		encoder.flush();
		bOut.close();
		
		
		
	}

}
