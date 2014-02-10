package gentestdata;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.NumberFormat;

public class GenTestDataForLoadStoreUDF {

	final static String datFile = "bytes.dat";
	final static String tocFile = "bytes.toc";

	public static void main(String[] args) throws IOException {
		NumberFormat nf = NumberFormat.getIntegerInstance();
		nf.setGroupingUsed(false);
		nf.setMinimumIntegerDigits(7);
		
		System.out.println(nf.format(5));
		//write();
		//read();
	}

	public static void write() throws IOException {
		String s = "Hello world";
		String s1 = "Hello world1";
		byte[] bytes = s.getBytes("UTF-8");
		byte[] bytes1 = s1.getBytes("UTF-8");
		FileWriter fw = new FileWriter(tocFile);
		DataOutputStream dos = new DataOutputStream(new PrintStream(datFile));

		for (byte b : bytes) {
			System.out.println(b);
			
		}

		/*
		 * for (byte b : bytes1) { System.out.println(b); dos.write(b);
		 * dos.flush(); }
		 */
		dos.write(bytes);
		dos.write(bytes1);
		fw.write(bytes.length + "\t");
		fw.write(bytes1.length + "\t");
		String newS = new String(bytes);
		System.out.println("\n" + newS);
		dos.close();
		fw.close();
	}

	public static void read() throws IOException {
		System.out.println("From write");
		BufferedReader tocReader = new BufferedReader(new FileReader(tocFile));
		DataInputStream dis = new DataInputStream(new FileInputStream(datFile));
		String line = tocReader.readLine();
		String tokens[] = line.trim().split("\t");
		byte[] b1 = new byte[Integer.parseInt(tokens[0])];
		dis.read(b1, 0, Integer.parseInt(tokens[0]));
		System.out.println(new String(b1));
		tocReader.close();
		dis.close();
	}
}
