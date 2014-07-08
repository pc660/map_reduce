package Message;

public class ReadMessage extends Message{
	public String ChunckName;
	//public int offset;
	//public int length;
	public int start;
	public int end;
	public ReadMessage (String ChunckName, int start, int end)
	{
		this.ChunckName = ChunckName;
		this.start = start;
		this.end = end;
	}
}
