package file;

import java.io.Serializable;

import DataNode.DataNodeInfo;

public class Chunck implements Serializable{
	
	public DFSfile file;
	
	public DataNodeInfo nodeInfo;
	public String chunckname;
	public boolean created = true;
	public int startIndex;
	public int endIndex;
	//public int max_size;
	public int id;
	public Chunck (DFSfile file, DataNodeInfo nodeinfo, String name, boolean create)
	{
		this.file = file; 
		this.nodeInfo = nodeinfo;
		this.chunckname = name;
		this.created = create;
	}
	public Chunck(DataNodeInfo nodeinfo)
	{
		this.nodeInfo = nodeinfo;
		created = true;
		startIndex = 0;
		endIndex = nodeinfo.chunck_size;
	}
	public Chunck (Chunck c)
	{
		this.file = c.file;
		this.nodeInfo = c.nodeInfo;
		this.chunckname = c.chunckname;
		this.created = c.created;
		this.startIndex = c.startIndex;
		this.endIndex = c.endIndex;
		this.id = c.id;
	}
	public String gethost()
	{
		return nodeInfo.hostname;
	}
	public int getuploadport()
	{
		return nodeInfo.upload_port;
	}
}
