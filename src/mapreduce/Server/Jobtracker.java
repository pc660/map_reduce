package mapreduce.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import MessageForMap.*;
import mapreduce.Jobconfig;

public class Jobtracker {

//	Jobconfig config;
	Jobmanager jobmanager ;
	TaskManager taskmanger;
	/*
	 * 
	 * */
	
	
	/*
	 * receive message from slave
	 * including register message and alive message
	 * and assign task  (if it is available)
	 * 
	 * */
	public Jobtracker ()
	{
		
	}
	
	private class jobreceiver extends Thread
	{
		ServerSocket server;
		public jobreceiver ()
		{
			try {
				server = new ServerSocket (10001);
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public void run()
		{
			while(true)
			{
				
				try {
					Socket ss = server.accept();
					ObjectInputStream input = new ObjectInputStream (ss.getInputStream());
					Message msg = (Message) input.readObject();
					
					if (msg instanceof JobMessage)
					{
						JobMessage m = (JobMessage) msg;
						jobmanager.add(  m.config);
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		
	}
	
	private class ResouceManager extends Thread
	{
		
		
	}
	
	
	
	
	
}
