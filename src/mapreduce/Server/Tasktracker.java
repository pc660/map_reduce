package mapreduce.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import MessageForMap.*;
//import MessageForMap.SlaveMessage;

public class Tasktracker {
	TaskManager manger; 
	public int taskport  = 20001;
	
	public Tasktracker ()
	{
		manger = new TaskManager(4);
		sendBeat sendbeat = new sendBeat();
		check check = new check();
		check.start();
		sendbeat.start();
	}
	
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
		ServerSocket server;
		public sendBeat()
		{
			try {
				server = new ServerSocket (taskport);
				s = new Socket ("127.0.0.1", 10002);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		
		/*
		 * schedule and run tasks
		 * 
		 * */
		
		public synchronized void schedule ()
		{
			ArrayList<Taskstatus> list = manger.getAvailableTasks();
			for (Taskstatus t : list)
			{
				if (t.type.equals("map"))
				{
					System.out.println("receive one map");
					manger.map_num ++ ;
					manger.map_slot--;
					t.state = Status.Running;
					//run task
				}
				else if (t.type.equals("reduce"))
				{
					System.out.println("receive one reduce");
					manger.reduce_num ++;
					manger.reduce_slot--;
					t.state = Status.Running;
					
					//run task
				}
				else
				{
					
					System.out.println("wierd");
				}
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
				ObjectInputStream input = new ObjectInputStream (s.getInputStream());
				Message m = (Message) input.readObject();
				
				if ( m instanceof TaskCreateMessage)
				{
					TaskCreateMessage task = (TaskCreateMessage) m;
					for (Taskconfig t : task.task)
						{
							manger.add( t);
						}
					schedule();
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
