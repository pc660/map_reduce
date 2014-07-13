package mapreduce.Server;

import java.util.HashMap;

public class TaskManager {
	public int map_num;
	public int reduce_num;
	public int cpu_num;
	public String root;
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
	public void add (Taskconfig config)
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
