package mapreduce.Server;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.text.Document;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import MessageForMap.*;
import mapreduce.Jobconfig;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
public class Jobtracker {

//	Jobconfig config;
	public int receiver_port = 10001;
	public int resource_port = 10002;
	Jobmanager jobmanager ;
	HashMap<String, TaskManager> taskmanger;
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
	
		File fXmlFile = new File("mapreduce.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		//System.out.println("123");
		
			try {
				dBuilder = dbFactory.newDocumentBuilder();
				org.w3c.dom.Document doc;
				doc = dBuilder.parse(fXmlFile);
				doc.getDocumentElement().normalize();
				NodeList nList = doc.getElementsByTagName("master");
				//System.out.println(nList.getLength());
				
				Node node = nList.item(0);
				Element eElement = (Element) node;
				//nodeInfo = new NameNodeInfo();
			//	hostname = eElement.getElementsByTagName("masterhost").item(0).getTextContent();
				receiver_port = Integer.parseInt(eElement.getElementsByTagName("receiver_port").item(0).getTextContent());
				resource_port = Integer.parseInt(eElement.getElementsByTagName("resource_port").item(0).getTextContent());
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		jobmanager = new Jobmanager();
		taskmanger = new HashMap<String, TaskManager>();
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
	private class keepalive extends Thread
	{
		@Override
		public void run()
		{
			while(true)
			{
				ArrayList<TaskManager> list = new ArrayList<TaskManager> ();
				for (String i : taskmanger.keySet())
				{
					TaskManager task = taskmanger.get(i);
					try {
						Socket s = new Socket (task.hostname, task.port);
						
						s.close();			
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						list.add(task);
						// TODO Auto-generated catch block
						//e.printStackTrace();
					}
					
					
					
				}
				
				/*
				 * according to the list containing hostname
				 * and set all process in the list as Runnable
				 * (info in tasklocation)
				 * */
				for (TaskManager task :list)
				{
					for (String s : task.tasks.keySet())
					{
						Taskstatus tmp = task.tasks.get(s);
						tmp.state = Status.Runnable;
						task.tasks.put(s, tmp);
					}
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
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
						System.out.println(m.config.filename);
						jobmanager.add(  m.config);
					}
					//System.out.println("")
					listAllJobs();
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
				//System.out.println(str);
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
						job.reducestate.put(str, map.get(str).state);
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
			
		//	for (Integer i )
			
			for (Integer i : jobmanager.jobQueue.keySet())
			{
				Jobstatus job = jobmanager.jobQueue.get(i);
				boolean judge = true;
				for (String str : job.reducestate.keySet())
				{
					if (job.reducestate.get(str) != Status.Finished)
						judge = false;
				}
				if (judge){
					job.status = Status.Finished;
					System.out.println("job " + i + " finished");
					jobmanager.jobQueue.remove(job);
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
				//System.o
				if (job == null)
				{
					System.out.println("No Available jobs now");
					return list;
				}
				Taskconfig task = jobmanager.assign_map(msg.hostname, job);
				
				if (task == null)
				{
					System.out.println("Job ID: " + job.job_id + "does not have available map");
					task = jobmanager.assign_reduce(job, msg.hostname);
					
					
					if (task == null)
					{
						System.out.println(job.map_finished);
						System.out.println("Job ID: " + job.job_id + "does not have available reduce");
					}
					else{
						
						list.add(task);
						//System.out.println( "Job"+ task.jobID + "reduce" + task.taskID );
					}
					}
				else{
					//System.out.println( "Job"+ task.jobID + "_map" + task.taskID );
					list.add(task);
				}
				
			}
			System.out.println("list size " + list.size());
			return list;
			
			
		}
		public synchronized int generateSlaveId()
		{
			return taskmanger.size();
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
			
			
			task.slave_id = generateSlaveId();
			String name = msg.hostname + ":" + msg.port;
			taskmanger.put(name, task);
			
			
		}
		public synchronized Message handleMessage(SlaveMessage msg)
		{
			ArrayList<Taskconfig>  list = null;
			
			
			//updateJob
			String name = msg.hostname + ":" + msg.port;
			if (taskmanger.containsKey(name) )
			{		
				list = schedule (msg);
				
			}
			else
			{
				System.out.println("Register one slave");
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
					System.out.println(1);
					ObjectInputStream input = new ObjectInputStream (socket.getInputStream());
					Message msg = (Message) input.readObject();
					if (msg instanceof SlaveMessage)
					{	
						/*
						 * check slave info and assign tasks
						 * 
						 * */
						
						System.out.println("receive msg");
						System.out.println(((SlaveMessage) msg).hostname + ":" +( (SlaveMessage)msg).port);
						
						Message m = handleMessage ((SlaveMessage)msg);
						ObjectOutputStream output = new ObjectOutputStream (socket.getOutputStream());
						output.writeObject(m);
						System.out.println("****************");
						
						
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
