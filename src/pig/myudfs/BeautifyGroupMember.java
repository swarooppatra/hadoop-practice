package pig.myudfs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class BeautifyGroupMember extends EvalFunc<String> {

	Log log = getLogger();

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() != 2) {
			return null;
		}
		try {
			System.out.println("Input size : " + input.size());
			int index = (Integer) input.get(1);
			StringBuffer sf = new StringBuffer();
			for (int i = 0; i < input.size(); i++) {
				Object o = input.get(i);
				if (o instanceof DataBag) {
					DataBag b = (DataBag) o;
					System.out.println("Bag size : " + b.size());
					for (Tuple t : b) {
						System.out.println("Element : " + t.get(index - 1));
						sf.append(t.get(index - 1));
						sf.append(",");
					}
				}
			}
			sf.deleteCharAt(sf.lastIndexOf(","));
			return sf.toString();
		} catch (Exception e) {
			throw new IOException("Exception in processing " + e);
		}
	}

}
