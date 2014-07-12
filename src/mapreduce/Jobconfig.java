package mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Jobconfig implements Serializable {
	
	//public HashMap<String, Class> configure_Class; 
	//public ArrayList<Byte> jar;
	public String jobtype; 
	
	/*
	 * constructor
	 *
	 * */
	
	

	public Class mapClass;
	public Class reduceClass;
	
	public String filename;
	
	public Class mapInputKeyClass;
	public Class mapInputValueClass;
	public Class mapOutputKeyClass;
	public Class mapOutputValueClass;

	public Class reduceInputKeyClass;
	public Class reduceInputValueClass;
	public Class reduceOutputKeyClass;
	public Class reduceOutputValueClass;
	public String jobName;
	//public String jobtype;
	public ArrayList<Byte> jar ;
	
}
