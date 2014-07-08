package Message;

import java.util.ArrayList;

import file.DFSfile;

public class WriteMessage extends Message{
	public String chunck_name;
	//public byte [] buffer;
	public ArrayList<String> text;
	//public DFSfile file;
	public WriteMessage(String name, ArrayList<String> text)
	{
		this.chunck_name = name;
		this.text = text;
	}
}
