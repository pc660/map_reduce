package mapreduce.Server;

import java.util.HashMap;

public class TaskManager {
	public int map_num;
	public int reduce_num;
	public int cpu_num;
	public String root;
	public int map_slot;
	public int reduce_slot;
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
	
	
	HashMap<Integer, Taskstatus > manager;
	
}
