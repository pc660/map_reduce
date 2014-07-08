package Message;

import java.io.Serializable;

public class Message implements Serializable{
	private String filename;
	
	public String getName()
	{
		return filename;
	}
}
