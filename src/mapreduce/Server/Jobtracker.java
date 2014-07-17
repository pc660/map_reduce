package mapreduce.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import MessageForMap.*;
import mapreduce.Jobconfig;

public class Jobtracker {

//	Jobconfig config;
	public int receiver_port = 10001;
	public int resource_port = 10002;
	Jobmanager jobmanager ;
	HashMap<Integer, TaskManager> taskmanger;
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
		jobmanager = new Jobmanager();
		taskmanger = new HashMap<Integer, TaskManager>();
		jobreceiver recever = new jobreceiver (receiver_port);
		recever.start();
		ResouceManager manager = new ResouceManager(resource_port);
		manager.start();
	}
	public void listAllJobs()
	{
		for (Integer i: jobmanager.jobQueue.keySet())
		{
			Jobstatus job = jobmanager.jobQueue.get(i);
			System.out.println("Job id: " + i );
			System.out.println("map num: " + job.map_num);
			System.out.println("reduce num: " + job.reduce_num);
			System.out.println(job.unassigned_map);
			System.out.println(job.unassigned_reduce);
			System.out.println("************");
			
		}
	}
	private class jobreceiver extends Thread
	{
		ServerSocket server;
		public jobreceiver ( int port)
		{
			try {
				server = new ServerSocket (port);
				
				
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
						//System.out.println(m.config.filename);
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
		ServerSocket server;
		public ResouceManager (int port)
		{
			try {
				server = new ServerSocket (port);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public synchronized void updateJobStatus (SlaveMessage msg)
		{
			HashMap<String, Taskstatus> map = msg.tasks;
			
			for (String str : map.keySet() )
			{
				System.out.println(str);
				String [] args = str.split("_");
				int id = Integer.parseInt(args[0].substring(3));
				Jobstatus job = jobmanager.jobQueue.get(id);
				if (args[1].contains("map"))
				{
					if (job.mapstate.containsKey(str))
					{
						job.mapstate.put(str, map.get(str).state);
					}
					else 
					{
						System.out.println("wierd in update");
					}
				}
				else if (args[1].contains("reduce") )
				{
					if (job.reducestate.containsKey(str))
					{
						job.mapstate.put(str, map.get(str).state);
					}
					else 
					{
						System.out.println("wierd in update");
					}
				}
			}
			
			for (Integer i : jobmanager.jobQueue.keySet())
			{
				Jobstatus job = jobmanager.jobQueue.get(i);
				boolean judge = true;
				for (String str : job.mapstate.keySet())
				{
					if (job.mapstate.get(str) != Status.Finished)
						judge = false;
				}
				if (judge){
					job.map_finished = true;
					System.out.println("job " + i + " map finished");
				}
			}
			
			
			
		}
		
		public ArrayList<Taskconfig> schedule (SlaveMessage msg)
		{
			
			//update job infor
			updateJobStatus (msg);
			
			//check available slots
			ArrayList<Taskconfig > list = new ArrayList<Taskconfig> ();
			System.out.println("cpu " + msg.available_cpu );
			for (int i = 0; i< msg.available_cpu; i++)
			{
				Jobstatus job = jobmanager.getFirstAvailableJob();
				
				if (job == null)
				{
					System.out.println("No Available jobs now");
					return list;
				}
				Taskconfig task = jobmanager.assign_map(msg.hostname, job);
				
				if (task == null)
				{
					System.out.println("Job ID: " + job.job_id + "does not have available map");
					task = jobmanager.assign_reduce(job);
					if (task == null)
					{
						System.out.println("Job ID: " + job.job_id + "does not have available reduce");
					}
					else{
						list.add(task);
						System.out.println( "Job"+ task.jobID + "reduce" + task.taskID );
					}
					}
				else{
					System.out.println( "Job"+ task.jobID + "_map" + task.taskID );
					list.add(task);
				}
				
			}
			System.out.println("list size " + list.size());
			return list;
			
			
		}
		public void register (SlaveMessage msg)
		{
			System.out.println("register");
			TaskManager task = new TaskManager(msg.CPU);
		//	task.cpu_num = msg.CPU;
			task.hostname = msg.hostname;
			task.port = msg.port;
			//task.map_num = msg.map_slot;
			HashMap<String, Taskstatus> tasks = msg.tasks;
			for (String str : tasks.keySet())
			{
				Taskstatus current = tasks.get(str);
				if (current.type == "map")
				{
					task.map_num ++;
				}
				else if (current.type == "reduce")
				{
					task.reduce_num ++;
				}
			}	
			task.root = msg.root;
			task.map_slot = msg.map_slot;
			task.reduce_slot = msg.reduce_slot;
			task.slave_id = msg.slaveID;
			taskmanger.put(msg.slaveID, task);
			
			
		}
		public synchronized Message handleMessage(SlaveMessage msg)
		{
			ArrayList<Taskconfig>  list = null;
			
			
			//updateJob
			
			if (taskmanger.containsKey(msg.slaveID) )
			{		
				list = schedule (msg);
				
			}
			else
			{
				register(msg);
				list = schedule(msg);
			}
			//send message back to slave
			TaskCreateMessage m = new TaskCreateMessage ();
			//System.out.println(list.size());
			m.task = list;
			return m;
			
			
		}
		@Override
		public void run()
		{
			while (true)
			{
				try {
					Socket socket = server.accept();
					ObjectInputStream input = new ObjectInputStream (socket.getInputStream());
					Message msg = (Message) input.readObject();
					if (msg instanceof SlaveMessage)
					{	
						/*
						 * check slave info and assign tasks
						 * 
						 * */
						System.out.println("receive msg");
						Message m = handleMessage ((SlaveMessage)msg);
						ObjectOutputStream output = new ObjectOutputStream (socket.getOutputStream());
						output.writeObject(m);
						
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
	
	
	
	
	
}
