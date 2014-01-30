package pig.myudfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class Mean extends EvalFunc<Double> {

	public String getInitial() {
		return null;
	}

	public String getIntermed() {
		return null;
	}

	public String getFinal() {
		return Final.class.getName();
	}

	@Override
	public Double exec(Tuple arg0) throws IOException {
		return mean(arg0);
	}

	public class Final extends EvalFunc<Tuple> {

		@Override
		public Tuple exec(Tuple input) throws IOException {
			return TupleFactory.getInstance().newTuple(mean(input));
		}
	}

	protected Double mean(Tuple t) throws ExecException {
		System.out.println(t);
		double sum = 0, count = 0;
		Object o = t.get(0);
		if (o instanceof DataBag) {
			DataBag bag = (DataBag) o;
			for (Tuple tuple : bag) {
				if (tuple != null && tuple.size() != 0 && tuple.get(0) != null) {
					Object element = tuple.get(0);
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
						reporter.progress("Count : "+count);
					} else {
						warn("Found a non-numeric value",
								PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED);
						// return null;
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
