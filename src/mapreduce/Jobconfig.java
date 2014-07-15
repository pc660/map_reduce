package mapreduce;

import java.io.*;
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
	public void setJar (String filename)
	{
		try {
			//FileOutputStream output = new FileOutputStream(new File(filename));
			
			FileInputStream br = new FileInputStream(filename);
			int count = br.available();
	       // System.out.println(count);
	         // create buffer
	        byte[] bs = new byte[count];
	        br.read(bs);
	        for(int i = 0; i< bs.length; i++)
	        {
	        	jar.add(bs[i]);
	        }
	        this.filename = filename;
	        
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public Jobconfig()
	{
		jar = new ArrayList<Byte>();
	}

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
	public String mapInputPath;
	//public String jobtype;
	public ArrayList<Byte> jar ;
	
}
