package ding;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class ReduceRecordReader extends RecordReader<String, String> {
	private String pathname;
	private File file;
	private KVPair currentKVPair;
	private Scanner scanner;
	private ArrayList<String> log;
	public ReduceRecordReader(String pathname, ArrayList<String> log) {
		this.pathname = pathname;
		file = new File(pathname);
		currentKVPair = null;
		this.log = log;
		//System.out.println("reduce record reader " + pathname);
	}
	
	public ReduceRecordReader(String pathname)
	{
		this.pathname = pathname;
		file = new File (pathname);
		System.out.println("reduce record reader " + pathname);
		currentKVPair = null;
		log = new ArrayList<String >();
	}
	@Override
	
	public boolean init() {
		try {
			
			scanner = new Scanner(this.file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			log.add("failed during scanner = new Scanner(this.file) in ReduceRecordReader");
			return false;
		}
		return true;
	}
	
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextKeyVlaue() throws Exception {
		// TODO Auto-generated method stub
		System.out.println("aslkdhaskldjalksjdklsj");
		if (!scanner.hasNextLine()) 
		{
			System.out.println("no next line");
			currentKVPair = null;
			return false;
		}
		
		String currentLine = scanner.nextLine();
		currentKVPair = new KVPair(currentLine);
		return true;
	}

	@Override
	public String getCurrentKey() {
		// TODO Auto-generated method stub
		return currentKVPair.getKey();
	}

	@Override
	public String getCurrentValue() {
		// TODO Auto-generated method stub
		return currentKVPair.getValue();
	}

}
