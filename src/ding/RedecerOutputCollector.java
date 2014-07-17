package ding;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class RedecerOutputCollector<K, V> implements OutputCollector<K, V>{
	private String outputFilename;
	private PrintWriter outputFilePrinter;
	private File resultFile;
	private ArrayList<String> log;
	
	public RedecerOutputCollector(String outputFilename, ArrayList<String> log) {
		this.outputFilename = outputFilename;
		this.log = log;
		this.resultFile = new File(outputFilename);

	}
	
	public RedecerOutputCollector(String outputFilename) {
		this.outputFilename = outputFilename;
		this.log = new ArrayList<String>();
		this.resultFile = new File(outputFilename);

	}
	public boolean init() {
		try {
			this.outputFilePrinter = new PrintWriter(resultFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			log.add("Failed during init() in RedecerOutputCollector");
			return false;
		}
		return true;
	}
	
	@Override
	public void collect(K key, V value) throws IOException {
		// TODO Auto-generated method stub
		String line = key.toString() + "\t" + value.toString();
		this.outputFilePrinter.println(line);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		this.outputFilePrinter.close();
		this.outputFilePrinter = null;
	}

}
