package mapreduce.Server;

import java.util.ArrayList;
import java.util.HashMap;

public class TaskManager {
	
	
	public int map_num;
	public int reduce_num;
	public int cpu_num;
	public String root;
	public String hostname;
	public int port;
	public int slave_id;
	//public HashMap<Integer, Taskstatus > manager;
	public HashMap<String, Taskstatus>  tasks;
	public int map_slot; //maximum map slots
	public int reduce_slot; //maximum reduce slots
	public TaskManager(int cpu)
	{
		this.cpu_num = cpu;
		this.map_slot = cpu;
		this.reduce_slot = cpu;
		this.map_num = 0;
		this.reduce_num = 0;
		//this.manager = new HashMap<Integer, Taskstatus> ();
		this.tasks = new  HashMap<String, Taskstatus>();
	}
	public synchronized void add (Taskconfig config)
	{
		Taskstatus task = new Taskstatus ();
		task.jobId = config.jobID;
		task.taskId = config.taskID;
		task.state = Status.Runnable;
		task.type = config.jobtype;
		task.assign = false;
		task.config = config;
		if (task.type == "map")
		{
			this.map_num++;
			task.output = root + "/" + "map_tmp/" + task.jobId + "_" + task.taskId;
		}
		else if (task.type == "reduce")
		{
			this.reduce_num ++;
		}
		task.log = new ArrayList<String>();
		String id = "";
		if (task.type.equals("map"))
		 id =  "job" + task.jobId + "_map" + task.taskId;
		else if (task.type.equals("reduce"))
			 id =  "job" + task.jobId + "_reduce" + task.taskId;
		System.out.println(id);
		tasks.put(id, task);
	}
	
	public synchronized ArrayList<Taskstatus> getAvailableTasks( )
	{
		//first check dead task
		
		int slot = map_slot + reduce_slot;
		ArrayList<Taskstatus> list = new ArrayList<Taskstatus> ();
		for (String str : tasks.keySet())
		{
			System.out.println("id: " + str);
			Taskstatus task = tasks.get(str);
			if ( slot > 0 && task.assign == false && task.state == Status.Runnable)
			{
				list.add(task);
				slot -- ;
			}
			if (slot == 0)
				break;
			
		}
		return list;
		
		
	}
	public int free_map_slots()
	{
		return map_slot - map_num;
	}
	public int free_reduce_slots()
	{
		return reduce_slot - reduce_num;
	}
	
	
	
}
