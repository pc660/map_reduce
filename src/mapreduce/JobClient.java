package mapreduce;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import MessageForMap.JobMessage;

public class JobClient {
	public String hostname; 
	public int jobtrackerport;
	
	public void runJob(Jobconfig config) throws IOException
	{
		/*
		 * Send jobconfig to the master node
		 * */
		
		/*
		 * hard code here
		 * */
		JobMessage msg = new JobMessage();
		msg.config = config;
		
		Socket s = new Socket (hostname, jobtrackerport);
		
		ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
		output.writeObject(msg);
		
		
		
	}
}
