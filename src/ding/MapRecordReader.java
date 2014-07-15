package ding;

import java.io.IOException;
import java.net.UnknownHostException;

import File_system.ChunckReader;
import file.Chunck;

public class MapRecordReader extends RecordReader<String, String>{
	private ChunckReader chunckReader;
	private String line;
	private int lineNum;
	public MapRecordReader (Chunck chunck) {
		try {
			this.chunckReader = new ChunckReader(chunck);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.line = null;
		this.lineNum = 0;
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
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
