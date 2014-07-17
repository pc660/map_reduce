package MessageForMap;

import java.util.HashMap;
import mapreduce.Server.*;
public class SlaveMessage extends Message{
	public int slaveID;
	public int map_slot;
	public int reduce_slot;
	public int CPU;
	public int available_cpu;
	public HashMap<String, Taskstatus> tasks;
	public String hostname; 
	public int port;
	public String root;
	public HashMap<Integer, Taskstatus> task_list;

}
