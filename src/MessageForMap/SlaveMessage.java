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
	

}
