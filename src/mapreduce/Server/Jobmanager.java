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
	
	public synchronized void add (Jobconfig config)
	{
		String name = config.jobName;
		Jobstatus status = new Jobstatus();
		
		int id = genereateID();
		status.job_id = id;
		
		status.status = Status.Running;
		
		//get DFS
		status.jobConfig = config;
		status.mapstate = new HashMap<String, Status>();
		status.reducestate = new HashMap<String, Status>();
		status.mapinput = new HashMap<String, ArrayList<Chunck>  >();
		
		//get map and reduce num
		
		DistributedFileSystem dfs = new DistributedFileSystem();
		DFSfile file = dfs.getFile(config.filename);
		status.map_num = file.chuncklist.size();
	
		//hard code
		status.reduce_num = 1;
		
		//initilize map
		for (int i = 0; i< status.map_num ; i++)
		{
			String taskID = "job" + id + "_map" + i;
			status.mapstate.put(taskID, Status.Suspend);
			status.mapinput.put(taskID, file.chuncklist.get(i));
			
		}
		
		for (int i = 0; i< status.reduce_num;i++)
		{
			String taskID = "job" + id + "_reduce" + i;
			status.reducestate.put(taskID, Status.Suspend);
			
		}
		jobQueue.put(id, status);
	
		
	}
	public Jobconfig assign_map ()
	{
		 return null;
	}
	
	public Jobconfig assign_reduce()
	{
		return null;
	}
}
