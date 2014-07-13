package mapreduce.Server;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import MessageForMap.SlaveMessage;

public class Tasktracker {
	TaskManager manger; 
	
	
	/*
	 * manage task
	 * 
	 * */
	
	/*
	 * check each task states 
	 * (rerun failed task)
	 * */
	private class check extends Thread
	{
		
	}
	/*
	 * report current state to jobtracker
	 * */
	private class sendBeat extends Thread
	{
		Socket s;
		public void sendBeat()
		{
			try {
				s = new Socket ("127.0.0.1", 10002);
				
				
				
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public void run()
		{
			try {
				ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
				SlaveMessage msg = new SlaveMessage();
				msg.map_slot = manger.free_map_slots();
				msg.reduce_slot = manger.free_reduce_slots();
				msg.CPU = manger.cpu_num;
				msg.tasks = manger.tasks;
				output.writeObject(msg);
				
				/*
				 * Receive Result Message from job tracker
				 * 
				 * */
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
}
