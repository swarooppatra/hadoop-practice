package pig.myudfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

public class Mean extends EvalFunc<Double> {

	String detainedStudentsFile;
	List<String> detainedStudents = null;

	public Mean(String file) {
		detainedStudentsFile = file;
	}

	@Override
	public Double exec(Tuple arg0) throws IOException {
		if (detainedStudents == null) {
			detainedStudents = new ArrayList<String>();
			FileSystem fs = FileSystem.get(UDFContext.getUDFContext()
					.getJobConf());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(new Path(detainedStudentsFile))));
			String line = "";
			while ((line = br.readLine()) != null) {
				detainedStudents.add(line.trim());
			}
		}
		return mean(arg0);
	}

	protected Double mean(Tuple t) throws ExecException {
		System.out.println(t);
		double sum = 0, count = 0;
		Object o = t.get(0);
		if (o instanceof DataBag) {
			DataBag bag = (DataBag) o;
			for (Tuple tuple : bag) {
				if (tuple != null && tuple.size() == 3) {
					String empName = (String) tuple.get(1);
					if (!detainedStudents.contains(empName)) {
						Object element = tuple.get(2);
						if (element instanceof Number) {
							switch (DataType.findType(element)) {
							case DataType.DOUBLE:
								sum += (Double) element;
								break;
							case DataType.FLOAT:
								sum += (Float) element;
								break;
							case DataType.INTEGER:
								sum += (Integer) element;
								break;
							case DataType.LONG:
								sum += (Long) element;
								break;
							default:
								warn("Found a non-numeric value",
										PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED);
								// return null;
							}
							count++;
							reporter.progress("Count : " + count);
						} else {
							warn("Found a non-numeric value",
									PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED);
							// return null;
						}
					} else {
						warn("Found an detained student",
								PigWarning.UDF_WARNING_1);
					}
				}
			}
		}
		return sum / count;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			if (input == null || input.size() != 1
					|| input.getField(0).type != DataType.BAG) {
				throw new RuntimeException("Expected a Bag of numbers");
			}
		} catch (FrontendException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return new Schema(new FieldSchema(null, DataType.DOUBLE));
	}

	public static void main(String args[]) {
		int i = 13;
		Object iO = new Integer(i);
		System.out.println();
		double d = (Integer) iO;
		System.out.println(d);
	}

}
