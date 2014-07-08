package Message;

import file.DFSfile;

public class GetChunckMessage extends Message{
	public DFSfile file;
	public GetChunckMessage(DFSfile file)
	{
		this.file = file;
	}
	public DFSfile getfile()
	{
		return file;
	}
}
