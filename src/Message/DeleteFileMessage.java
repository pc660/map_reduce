package Message;

public class DeleteFileMessage extends Message{
	public String filename;
	public DeleteFileMessage (String filename)
	{
		this.filename = filename;
	}
	public String filename()
	{
		return this.filename;
	}
}
