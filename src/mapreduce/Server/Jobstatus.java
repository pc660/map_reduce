package mapreduce.Server;

import java.io.Serializable;
import java.util.HashMap;

import mapreduce.Jobconfig;

public class Jobstatus implements Serializable {

	/*
	 * running, waiting, kill, success, fail
	 * */
	public String status;
	public int job_id;
	
	public int map_num;
	public int reduce_num;
	
	
	public HashMap<String, Status> mapstate;
	public HashMap<String, String> reduceinput;
	
	public HashMap<String, Status> reducestate;
	public Jobconfig jobConfig;
	
	
	
}
