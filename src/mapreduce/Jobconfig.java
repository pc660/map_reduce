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
	        this.jarname = filename;
	        
	        String new_name = filename.substring(0, filename.length() -6);
	        String map_name = new_name + "$map.class";
	        String reduce_name = new_name + "$reduce.class";
	        System.out.println(map_name);
	        
	        br = new FileInputStream(map_name);
	        count = br.available();
		       // System.out.println(count);
		         // create buffer
		     bs = new byte[count];
		        br.read(bs);
		        for(int i = 0; i< bs.length; i++)
		        {
		        	map_data.add(bs[i]);
		        }
		        this.map_name = map_name;
		        
		        br = new FileInputStream(reduce_name);
		        count = br.available();
			       // System.out.println(count);
			         // create buffer
			     bs = new byte[count];
			        br.read(bs);
			        for(int i = 0; i< bs.length; i++)
			        {
			        	reduce_data.add(bs[i]);
			        }
			        this.reduce_name = reduce_name;
			        
			        System.out.println(jar.size());
			        System.out.println(map_data.size());
			        System.out.println(reduce_data.size());
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
		map_data = new ArrayList<Byte>();
		reduce_data = new ArrayList<Byte>();
	}
	
	//public Class mapClass;
	//public Class reduceClass;
	
	public String filename;
	public String jarname;
	public Class mapInputKeyClass;
	public Class mapInputValueClass;
	public Class mapOutputKeyClass;
	public Class mapOutputValueClass;
	//public Class jobClass;
	public Class reduceInputKeyClass;
	public Class reduceInputValueClass;
	public Class reduceOutputKeyClass;
	public Class reduceOutputValueClass;
	public String jobName;
	public String mapInputPath;
	public String map_name;
	public String reduce_name;
	public String classname;
	
	//public String jobtype;
	public ArrayList<Byte> jar ;
	public ArrayList<Byte> map_data;
	public ArrayList<Byte> reduce_data;
}
