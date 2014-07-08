package Message;

public class AckMessage extends Message{
	public boolean finished;
	public String from_name;
	public String error_code;
	public AckMessage(boolean value, String from_name)
	{
		finished = value;
		this.from_name = from_name;
	}
}
