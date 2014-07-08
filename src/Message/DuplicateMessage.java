package Message;
import DataNode.DataNodeInfo;
import java.util.ArrayList;
public class DuplicateMessage extends Message{
	public ArrayList<DataNodeInfo> node_list; 
	public String chunck_name; 
	public DuplicateMessage (String chunck_name, ArrayList<DataNodeInfo> node_list)
	{
		this.chunck_name = chunck_name;
		this.node_list = node_list;
	}
	
}
