package mapreduce.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import ding.Mapper;
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
		@Override
		public void run(){
			while(true)
			{
				showTasks();
				removeSucceedTasks();
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		public synchronized void removeSucceedTasks()
		{
			for(String i : manger.tasks.keySet())
			{
				Taskstatus task = manger.tasks.get(i);
				if(task.state == Status.Succeed)
				{
					if(task.type.equals("map"))
					{
						manger.map_num--;
						task.state = Status.Finished;
						manger.map_slot++;
					}
					else if (task.type.equals("reduce"))
					{
						manger.reduce_num--;
						manger.reduce_slot++;
						task.state = Status.Finished;
					}
					else
					{
						System.out.println("Remove Success Tasks wired");
					}
				}				
			}
			
			System.out.println("Running map " + manger.map_num);
			System.out.println("Running reduce " + manger.reduce_num);
			
			
			
		}
		
		public void showTasks ()
		{
			for(String i : manger.tasks.keySet())
			{
				Taskstatus task = manger.tasks.get(i);
				
				System.out.println("Job ID: " + task.jobId + " task ID: " + task.taskId + " task job type + " + task.type + " status " + task.state.toString()    );
			}
		}
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
				//s = new Socket ("127.0.0.1", 10002);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		
		/*
		 * schedule and run tasks
		 * 
		 * */
		
		public  void schedule ()
		{
			ArrayList<Taskstatus> list = manger.getAvailableTasks();
			System.out.println("list size in schedule" + list.size());
			for (Taskstatus t : list)
			{
				if (t.type.equals("map"))
				{
					System.out.println("receive one map");
					manger.map_num ++ ;
					manger.map_slot--;
					t.state = Status.Runnable;
					
					//run task
					//t.config.localize();
					//Mapper map = (Mapper) t.config.getMapper();
					Task tmp = new Task ( t);
					try {
						System.out.println("Do mapper");
						Thread thread = tmp.do_mapper();
						thread.start();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					
					
				}
				else if (t.type.equals("reduce"))
				{
					System.out.println("receive one reduce");
					manger.reduce_num ++;
					manger.reduce_slot--;
					t.state = Status.Runnable;
					Task tmp = new Task ( t);
					System.out.println("Do reducer");
					Thread thread = tmp.do_reducer();
					thread.start();
					
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
			while(true){
			try {
				Socket s = new Socket ("128.237.199.44", 10002);
				ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
				SlaveMessage msg = new SlaveMessage();
				msg.hostname = server.getLocalSocketAddress().toString();
				if (msg.hostname.contains("0.0.0.0"))
					msg.hostname = "127.0.0.1";
				System.out.println(msg.hostname);
				msg.port = taskport;
				msg.map_slot = manger.free_map_slots();
				msg.reduce_slot = manger.free_reduce_slots();
				msg.CPU = manger.cpu_num;
				msg.tasks = manger.tasks;
				msg.available_cpu = manger.cpu_num - manger.map_num - manger.reduce_num;
				System.out.println(msg.available_cpu);
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
					System.out.println("receive task create message");
					TaskCreateMessage task = (TaskCreateMessage) m;
					System.out.println("task size " + task.task.size());
					//System.out.println("task  " + task.task.size());
					for (Taskconfig t : task.task)
						{
							System.out.println("task name " + t.jobtype + t.taskID);
							manger.add( t);
						}
					schedule();
				}
				Thread.sleep(1000);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			}
		}
	}
	
	
}
