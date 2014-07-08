package Message;

import java.util.HashMap;

import DataNode.DataNodeInfo;
import file.Chunck;

public class DatanodeAckMessage extends Message{
	public DataNodeInfo info;
	public HashMap<String, Chunck> map;
	public DatanodeAckMessage(DataNodeInfo info)
	{
		this.info = info;
	}
}
