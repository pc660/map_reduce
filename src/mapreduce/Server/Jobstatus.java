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
	
	public int map_num;
	public int reduce_num;
	
	//public HashMap<String, >
	public HashMap<String, ArrayList<Chunck> > mapinput;
	public HashMap<String, Status> mapstate;
	public HashMap<String, String> reduceinput;
	
	public HashMap<String, Status> reducestate;
	public Jobconfig jobConfig;
	
	
	
}
