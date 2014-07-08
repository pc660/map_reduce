package DataNode;

import java.io.Serializable;

public class DataNodeInfo implements Serializable{
	public String hostname;
	public int download_port;
	public int read_port;
	public int upload_port;
	public int name_port;
	public String rootdirectory;
	public int chunck_size;
	
}
