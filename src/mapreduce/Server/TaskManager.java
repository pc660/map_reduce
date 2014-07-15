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
		manager = new HashMap<Integer, Taskstatus> ();
		
	}
	public synchronized void add (Taskconfig config)
	{
		Taskstatus task = new Taskstatus ();
		task.jobId = config.jobID;
		task.taskId = config.TaskID;
		task.state = Status.Running;
		task.type = config.jobtype;
		
		if (task.type == "map")
		{
			this.map_num++;
			task.output = root + "/" + "map_tmp/" + task.jobId + "_" + task.taskId;
		}
		else if (task.type == "reduce")
		{
			this.reduce_num ++;
		}
		manager.put(task.taskId, task);
	}
	
	public synchronized ArrayList<Taskstatus> getAvailableTasks( )
	{
		//first check dead task
		
		int slot = map_slot + reduce_slot;
		ArrayList<Taskstatus> list = new ArrayList<Taskstatus> ();
		for (String str : tasks.keySet())
		{
			Taskstatus task = tasks.get(str);
			if ( slot > 0 && task.assign == false && task.state != Status.Succeed )
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
	
	HashMap<Integer, Taskstatus > manager;
	
}
