package Message;

public class GetMessage extends Message{
	public String filename;
	
	public GetMessage (String filename)
	{
		this.filename = filename;
	}
	
	@Override
	public String getName()
	{
		return filename;
	}
	
}
