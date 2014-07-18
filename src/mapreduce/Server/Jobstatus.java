package mapreduce.Server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import mapreduce.Jobconfig;
import file.*;
public class Jobstatus implements Serializable {

	/*
	 * running, waiting, kill, success, fail
	 * */
	public Status status;
	public int job_id;
	public int unassigned_map;
	public int unassigned_reduce;
	public int map_num;
	public int reduce_num;
	public boolean map_finished;
	//public HashMap<String, >
	public HashMap<String, ArrayList<Chunck> > mapinput;
	public HashMap<String, Status> mapstate;
	public HashMap<String, ArrayList<String > > reduceinput;
	//public ArrayList<String> reduceinput;
	//public HashMap<String, String> reduceinput;
	//ArrayList><
	public HashMap<String, Status> reducestate;
	public Jobconfig jobConfig;
	public HashMap<String, String> Tasklocations;
	
	public Jobstatus ()
	{
		
		Tasklocations = new HashMap<String, String> ();
		reducestate = new HashMap<String, Status>();
		reduceinput = new  HashMap<String, ArrayList<String > > ();
		mapstate = new HashMap<String, Status>();
		mapinput = new HashMap<String, ArrayList<Chunck> >();
		
		
	}
	
	
	//public HashMap<String, Class> jobio;
	
	
	
}
