package gentestdata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;

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
		NumberFormat nf1 = NumberFormat.getIntegerInstance();
		nf1.setGroupingUsed(false);
		nf1.setMinimumIntegerDigits(7);

		NumberFormat nf2 = NumberFormat.getIntegerInstance();
		nf2.setGroupingUsed(false);
		nf2.setMinimumIntegerDigits(2);
		nf2.setMaximumFractionDigits(1);

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
					/*
					 * 28 characters 7 char - index(long) 8 char - name(string)
					 * 2 char - dept id(short) 3 char - skill 1(string) starts
					 * with s 3 char - skill 2(string) starts with s 3 char -
					 * skill 3(string) starts with s 2 char - rank (short)
					 */

					while (write) {
						out.write(nf1.format(++index)
								+ "n"
								+ nf1.format(index)
								+ ""
								+ nf2.format(Math.ceil(Math.random() * 10))
								+ "s"
								+ nf2.format(Math.ceil(Math.random() * 10))
								+ "s"
								+ nf2.format(Math.ceil(Math.random() * 10))
								+ "s"
								+ nf2.format(Math.ceil(Math.random() * 10))
								+ ""
								+ nf2.format((int) Math.ceil(Math.random() * 99))
								+ "\n");
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
