package mapreduce.Server;

import java.util.HashMap;
import file.*;
import java.util.*;
import file.DFSfile;
import File_system.DistributedFileSystem;
import mapreduce.Jobconfig;

/*
 * manage jobs in job tracker
 * */
public class Jobmanager {

	
	
	HashMap<Integer, Jobstatus> jobQueue;
	
	public Jobmanager ()
	{
		jobQueue = new HashMap<Integer, Jobstatus>();
	}
	public  int genereateID()
	{
		for (int i = 0; i< jobQueue.size();i++)
		{
			if (!jobQueue.containsKey(i))
				return i;
		}
		return jobQueue.size();
		//return jobQueue.size();
	}
	//public void getAvailableTasks
	public synchronized void add (Jobconfig config)
	{
		String name = config.jobName;
		Jobstatus status = new Jobstatus();
		
		int id = genereateID();
		status.job_id = id;
		status.jobio = new HashMap<String, Class>();
		status.status = Status.Runnable;
		status.reduceinput = new HashMap<String, String>();
		//get DFS
		status.jobConfig = config;
		status.mapstate = new HashMap<String, Status>();
		status.reducestate = new HashMap<String, Status>();
		status.mapinput = new HashMap<String, ArrayList<Chunck>  >();
		
		//get map and reduce num
		
		DistributedFileSystem dfs = new DistributedFileSystem();
		DFSfile file = dfs.getFile(config.filename);
		status.map_num = file.chuncklist.size();
		System.out.println("map num " + status.map_num);
		status.unassigned_map = status.map_num;
		//hard code
		status.reduce_num = status.map_num;
		status.unassigned_reduce = status.reduce_num;
		//initilize map
		for (int i = 0; i< status.map_num ; i++)
		{
			String taskID = "job" + id + "_map" + i;
			status.mapstate.put(taskID, Status.Runnable);
			status.mapinput.put(taskID, file.chuncklist.get(i));
			for (int j = 0; j< status.reduce_num; j++)
			{
				String reduce_input = config.filename + "_chunck" + i  + "_" + j;
				String reduce_name = "job" + status.job_id + "_reduce" + j;
				status.reduceinput.put(reduce_name, reduce_input);
			}
	//		String name = config.filename + "_chunck" + i;
	//		for ()
		}
		
		for (int i = 0; i< status.reduce_num;i++)
		{
			String taskID = "job" + id + "_reduce" + i;
			status.reducestate.put(taskID, Status.Runnable);	
		}
		jobQueue.put(id, status);
	
		
	}
	public synchronized Jobstatus getFirstAvailableJob()
	{
		Jobstatus job = null;
		for (Integer i :jobQueue.keySet())
		{
			job = jobQueue.get(i);
			if (job.status == Status.Runnable )
				return job;
			else if (job.status == Status.Running &&  (job.unassigned_map >0|| job.unassigned_reduce>0) )
				return job;
		}
		
		return job;
	}
	public Taskconfig assign_map (String hostname, Jobstatus job)
	{
		//return a map with the corresponding hostname
		//if not exist, return the first one
		Taskconfig task = null;
		String pos = null; 
		boolean judge = false;
		if (job.unassigned_map == 0 )
			return null;
		else
		{
			for (String str : job.mapstate.keySet())
			{
				//System.out.println("123");
				if (job.mapstate.get(str) == Status.Runnable   )
				{
				//	System.out.println("123");
					ArrayList<Chunck> list = job.mapinput.get(str);
					for (Chunck tmp : list)
					{
						//System.out.println("1234");
						if(hostname.equals( tmp.nodeInfo.hostname) )
						{
							System.out.println("Find one task with same hostname");
							task = new Taskconfig();
							task.jobtype = "map";
							task.jar = job.jobConfig.jar;
							task.config = job.jobConfig;
							String [] args = str.split("_");
							
							task.taskID = Integer.parseInt(args[1].substring(3));
							task.jobID = Integer.parseInt(args[0].substring(3));
							task.mapinput = tmp;
							task.numOfRed = job.reduce_num;
							job.unassigned_map --;
							job.mapstate.put(str, Status.Suspend);
							return task;
						}
						else
						{
							if (!judge){
								System.out.println("Find one task with different hostname");
								judge = true;
								task = new Taskconfig();
								task.jobtype = "map";
								task.jar = job.jobConfig.jar;
								task.config = job.jobConfig;
								String [] args = str.split("_");
								task.numOfRed = job.reduce_num;
								task.taskID = Integer.parseInt(args[1].substring(3));
								task.jobID = Integer.parseInt(args[0].substring(3));
								task.mapinput = tmp;
								pos = str;
								//task.inputfile.add(tmp.chunckname);
							}
						}
					}
				}
			}
		}
		if(task != null){
			job.mapstate.put(pos, Status.Suspend);
			job.unassigned_map --;
		}
		return task;
	}
	
	public Taskconfig assign_reduce(Jobstatus job)
	{
		Taskconfig task = null;
		if (job.unassigned_reduce == 0 )
			return null;
	//	boolean judge = false;
		if (job.status == Status.Runnable && job.map_finished == true)
		{
			for (String str : job.reducestate.keySet())
			{
				if (job.reducestate.get(str) == Status.Runnable   )
				{
					//ArrayList<Chunck> list = job.reducestate.get(str);
					//HashMap<String, String> input = job.reduceinput;
					System.out.println("Assign one reduce");
					task = new Taskconfig();
					task.jobtype = "reduce";
					task.jar = job.jobConfig.jar;
					task.numOfRed = job.reduce_num;
					task.config = job.jobConfig;
					String [] args = str.split("_");
					
					task.taskID = Integer.parseInt(args[1].substring(6));
					task.jobID = Integer.parseInt(args[0].substring(3));
					task.inputfile = job.reduceinput;
					job.unassigned_reduce --;
					job.reducestate.put(str, Status.Suspend);
					return task;
					
					
				}
			}
		}
		return task;
	}
}
