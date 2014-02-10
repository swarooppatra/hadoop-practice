package pig.myudfs;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

public class LoadEmpData extends LoadFunc implements LoadMetadata {

	protected RecordReader reader = null;
	protected String udfSignature = null;

	private final String SCHEMA_STR = "pig.myudf.loademp";

	private TupleFactory tupleFac = TupleFactory.getInstance();

	public String[] getPartitionKeys(String arg0, Job job) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public ResourceSchema getSchema(String arg0, Job job) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public ResourceStatistics getStatistics(String arg0, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setPartitionFilter(Expression arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		Text value = null;
		try {
			if (!reader.nextKeyValue())
				return null;
			value = (Text) reader.getCurrentValue();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		String val = value.toString();
		Tuple t = tupleFac.newTuple(7);
		t.set(0, Long.parseLong(val.substring(0, 7)));
		t.set(1, val.substring(7, 15));
		t.set(2, Integer.parseInt(val.substring(15, 17)));
		t.set(3, val.substring(17, 20));
		t.set(4, val.substring(20, 23));
		t.set(5, val.substring(23, 26));
		t.set(6, Integer.parseInt(val.substring(26, 28)));
		return t;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = reader;
		UDFContext udfContext = UDFContext.getUDFContext();
		Properties prop = udfContext.getUDFProperties(this.getClass(),
				new String[] { udfSignature });
		String schema = prop.getProperty(SCHEMA_STR);
		ResourceSchema resourceSchema = new ResourceSchema(
				Utils.getSchemaFromString(schema));
		
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.addInputPath(job, new Path(location));

	}

	@Override
	public void setUDFContextSignature(String signature) {
		udfSignature = signature;
	}

}
