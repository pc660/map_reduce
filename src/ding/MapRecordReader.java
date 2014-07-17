package ding;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import File_system.ChunckReader;
import file.Chunck;

public class MapRecordReader extends RecordReader<String, String>{
	private ChunckReader chunckReader;
	private String line;
	private int lineNum;
	private ArrayList<String> log;
	private String chunckName;
	public Chunck chunck;
	public MapRecordReader (String chunckName, ArrayList<String> log) {
		this.log = log;
		this.chunckName = chunckName;
		this.line = null;
		this.lineNum = 0;
	}
	public MapRecordReader (Chunck chunck, ArrayList<String> log) {
		this.log = log;
		this.chunck = chunck;
		this.line = null;
		this.lineNum = 0;
	} 
	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
	

	
	@Override
	public boolean init() {
		try {
			this.chunckReader = new ChunckReader(this.chunck);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			log.add("Failed during init ChunckReader in MapRecordReader");
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.add("Failed during init ChunckReader in MapRecordReader");
			return false;
		}
		return true;
	}
	@Override
	public boolean nextKeyVlaue() throws Exception {
		// TODO Auto-generated method stub
		this.line = chunckReader.readline();
		this.lineNum++;
		return line != null;
	}

	@Override
	public String getCurrentKey() {
		// TODO Auto-generated method stub
		return this.lineNum+"";
	}

	@Override
	public String getCurrentValue() {
		// TODO Auto-generated method stub
		return this.line;
	}
	

}
