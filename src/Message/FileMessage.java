package Message;

import java.util.ArrayList;

public class FileMessage extends Message{
	public String Chunck_name;
	public ArrayList<String> text;
	public FileMessage (String Chunck_name,  ArrayList<String> text)
	{
		this.Chunck_name = Chunck_name;
		this.text = text;
	}
}
