package mapreduce.Server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import mapreduce.Jobconfig;

public class Taskconfig implements Serializable{

	public int jobID;
	public int TaskID;
	public String jobtype;
	public ArrayList<Byte> jar;
	//public HashMap<String, String> Inputfile;
	ArrayList<String> inputfile;
	public Taskconfig()
	{
		inputfile = new ArrayList<String>();
	}
	Jobconfig config;
	
}
