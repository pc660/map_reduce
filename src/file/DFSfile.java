package file;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import DataNode.DataNodeInfo;
public class DFSfile implements Serializable{
	public String filename;
	//for each list, store a list of chunk map
	public ArrayList<    ArrayList< Chunck >   > chuncklist;
	public HashMap<DataNodeInfo, ArrayList<Integer> > nodemap;
	public void addChunck (Chunck file, int id)
	{
		if (chuncklist.size() <= id)
		{
			ArrayList<Chunck> tmp = new ArrayList<Chunck> ();
			tmp.add(file);
			chuncklist.add(tmp);
			
		}
		else
		{
			chuncklist.get(id).add(file);
			
		}
		DataNodeInfo d = file.nodeInfo;
		if (nodemap.containsKey(d))
		{
			nodemap.get(d).add(id);
		}
		else
		{
			ArrayList<Integer> arr = new ArrayList<Integer> ();
			arr.add(id);
			nodemap.put(d, arr);
		}
	}
	public DFSfile()
	{
		chuncklist = new ArrayList<    ArrayList< Chunck >   >();
		nodemap = new HashMap<DataNodeInfo, ArrayList<Integer> > ();
	}
	
}
