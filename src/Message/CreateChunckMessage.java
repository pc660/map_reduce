package Message;

import file.DFSfile;

public class CreateChunckMessage extends Message{
	public String filename;
	public CreateChunckMessage(String filename)
	{
		this.filename = filename;
	}
	public String getfile()
	{
		return filename;
	}
}
