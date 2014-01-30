package gentestdata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class generates test employee data for PIG UDF testing
 * 
 * @author swaroop.patra
 * 
 */
public class GenTestDataEmployee {

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Invalid use of program");
			System.err
					.println("Usage : gentestdata.GenTestDataEmployee <o/p_dir> <no_of_files> <size_of_each_file_in_mb>");
			System.exit(1);
		}
		String fileName = "emp";
		String fileExtn = ".tsv";
		File f = new File(args[0].trim());
		if (f.isDirectory()) {
			int count = 0, index = 0;
			if (f.canWrite()) {
				for (int i = 0; i < Integer.parseInt(args[1].trim()); i++) {
					boolean write = true;
					File file = new File(f, fileName + i + fileExtn);
					FileWriter out = new FileWriter(file);
					while (write) {
						out.write(++index + "\tn" + index + "\t"
								+ Math.ceil(Math.random() * 10) + "\ts"
								+ Math.ceil(Math.random() * 10) + "\ts"
								+ Math.ceil(Math.random() * 10) + "\ts"
								+ Math.ceil(Math.random() * 10) + "\t"
								+ (int)Math.ceil(Math.random() * 99) + "\n");
						out.flush();
						count++;
						if (count == 100) {
							if (file.length() > Integer
									.parseInt(args[2].trim()) * 1024 * 1024) {
								write = false;
							}
							count = 0;
						}
					}
					out.flush();
					out.close();
					System.out.println("File " + file.getAbsolutePath()
							+ " created");
				}

			} else {
				System.err.println("No write permission");
				System.exit(1);
			}
		} else {
			System.err.println("Not a dir");
			System.exit(1);
		}
	}
}
