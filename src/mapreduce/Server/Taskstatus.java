package mapreduce.Server;

import java.io.Serializable;

import mapreduce.Jobconfig;

public class Taskstatus implements Serializable{
	public int jobId;
	public int taskId;
	public Status state;
	public String type;
	public String output;
	public Taskconfig config;
	
	
	//newly add stuff
	public boolean assign = false;
}
