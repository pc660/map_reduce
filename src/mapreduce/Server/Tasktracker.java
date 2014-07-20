package mapreduce.Server;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import ding.Mapper;
import File_system.DistributedFileSystem;
import MessageForMap.*;
//import MessageForMap.SlaveMessage;
import NameNode.NameNodeInfo;

public class Tasktracker {
	TaskManager manger; 
	public int taskport  = 20001;
	public int hostport ;
	public String hostname;
	public int maximum_time = 10 ;
	public Tasktracker (int port)
	{
		this.hostname = "unix6.andrew.cmu.edu";
		hostport = 10002;
		this.taskport = port;
		manger = new TaskManager(4);
		sendBeat sendbeat = new sendBeat();
		check check = new check();
		check.start();
		sendbeat.start();
	}
	public Tasktracker ()
	{
		
		File fXmlFile = new File("mapreduce.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		//System.out.println("123");
		
			try {
				dBuilder = dbFactory.newDocumentBuilder();
				Document doc;
				doc = dBuilder.parse(fXmlFile);
				doc.getDocumentElement().normalize();
				NodeList nList = doc.getElementsByTagName("master");
				//System.out.println(nList.getLength());
				
				Node node = nList.item(0);
				Element eElement = (Element) node;
				//nodeInfo = new NameNodeInfo();
				hostname = eElement.getElementsByTagName("masterhost").item(0).getTextContent();
				hostport = Integer.parseInt(eElement.getElementsByTagName("resource_port").item(0).getTextContent());
				nList = doc.getElementsByTagName("task");
				node = nList.item(0);
				eElement = (Element) node;
				taskport = Integer.parseInt(eElement.getElementsByTagName("port").item(0).getTextContent());
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
		@SuppressWarnings("deprecation")
		public synchronized void removeSucceedTasks()
		{
			for(String i : manger.tasks.keySet())
			{
				
				Taskstatus task = manger.tasks.get(i);
				if (task.state == Status.Running)
				{
					task.running_time ++;
					System.out.println(task.running_time);
					if (task.running_time > maximum_time)
					{
						
						
						Task tmp = new Task ( task.config);
						task.retry ++ ;
						if (task.retry > 2)
						{
							task.t.destroy();
							task.state = Status.Fail;
							continue;
						}
						try {
							task.t.destroy();
							//System.out.println("Do mapper");
							if (task.type.equals("map")){
								Thread thread = tmp.do_mapper();
								task.t = thread;
								thread.start();
								task.running_time = 0;
							}
							else if (task.type.equals("reduce"))
							{
								Thread thread = tmp.do_reducer();
								task.t = thread;
								thread.start();
								task.running_time = 0;
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				if(task.state == Status.Succeed)
				{
				//	task.config.numOfRed
				//	task.
					if(task.type.equals("map"))
					{
						manger.map_num--;
						task.state = Status.Finished;
						manger.map_slot++;
					}
					else if (task.type.equals("reduce"))
					{
						System.out.println("*************");
						manger.reduce_num--;
						manger.reduce_slot++;
						task.state = Status.Finished;
						DistributedFileSystem dfs = new DistributedFileSystem();
						
						for (int j = 0; j< task.config.numOfRed;j++)
						{
							String filename = task.config.config.filename;
							System.out.println("Try to delete " + filename + "_chunck" + j + "_" + task.taskId);
							File file = new File (filename + "_chunck" + j + "_" + task.taskId);
							if (file.exists()){
								file.delete();
								System.out.println("Successfully delete it");
							}
							
							dfs.removeFile(filename + "_chunck" + j + "_" + task.taskId);
							
						}
						
//						task.jobId
//						task.taskId
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
						t.t = thread;
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
					t.t = thread;
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
				Socket s = new Socket (hostname, hostport);
				ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
				SlaveMessage msg = new SlaveMessage();
				msg.hostname = server.getLocalSocketAddress().toString();
				if (msg.hostname.contains("0.0.0.0"))
					msg.hostname = "127.0.0.1";
				//System.out.println(msg.hostname);
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
