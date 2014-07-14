package mapreduce.Server;

import java.io.Serializable;

import mapreduce.Jobconfig;

public class TaskStatus implements Serializable{
	public int jobId;
	public int taskId;
	public Status state;
	public enum JobType {MAP, REDUCE};
	
	public String output;
	public Jobconfig config;
	
	
	
}
