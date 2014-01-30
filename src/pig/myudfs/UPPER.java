package pig.myudfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class UPPER extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}
		try {
			String empName = (String) input.get(0);
			empName = empName.toUpperCase();
			return empName;
		} catch (Exception e) {
			throw new IOException("Exception while processing : " + e);
		}
	}
}
