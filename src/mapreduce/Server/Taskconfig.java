package mapreduce.Server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import file.Chunck;
import mapreduce.Jobconfig;

public class Taskconfig implements Serializable{

	public int jobID;
	public int taskID;
	public String jobtype;
	public ArrayList<Byte> jar;
	//public HashMap<String, String> Inputfile;
	public HashMap<String,  ArrayList <String >   > inputfile;
	public Chunck mapinput;
	public int numOfRed;
	public Taskconfig()
	{
		inputfile = new  HashMap<String ,ArrayList <String > >();
	}
	
	public Taskconfig(int jobID, int taskID, String jobtype, 
			HashMap<String,  ArrayList <String >   >  inputfile, int numOfRed, ArrayList<Byte> jar) {
		this.jobID = jobID;
		this.taskID = taskID;
		this.jobtype = jobtype;
		this.inputfile = inputfile;
		this.numOfRed = numOfRed;
		this.jar = jar;
	}
	
	Jobconfig config;
	
	
	
	//write jar file to local system
	public void localize()
	{
	
		File f = new File (this.config.jarname);
		
		try {
			if (!f.exists())
				f.createNewFile();
			FileOutputStream output = new FileOutputStream(new File(this.config.jarname));
			System.out.println(config.jar.size());
			for (Byte b : config.jar)
			{
		//		System.out.println(b);
				output.write(b);
			}
			output.close();
			
			
		 f= new File(this.config.map_name);
		
			if (!f.exists())
				f.createNewFile();
			 output = new FileOutputStream(new File(this.config.map_name));
			System.out.println(config.jar.size());
			for (Byte b : config.map_data)
			{
		//		System.out.println(b);
				output.write(b);
			}
			output.close();


			
			 f= new File(this.config.reduce_name);
		
				if (!f.exists())
					f.createNewFile();
				 output = new FileOutputStream(new File(this.config.reduce_name));
				System.out.println(config.jar.size());
				for (Byte b : config.reduce_data)
				{
				//	System.out.println(b);
					output.write(b);
				}
				output.close();
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public Object getMapper ()
	{
		localize();
		if (!jobtype.equals("map") )
			return null;
		else
		{
			
			try {
				//System.out.println(this.config.classname);
				Class t = Class.forName(this.config.classname + "$Map");
				return t.newInstance();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return null;
	}
	
	public Object getReducer()
	{
		localize();
		
		if (!jobtype.equals("reduce") )
			return null;
		else
		{
			Class t;
			try {
				t = Class.forName(this.config.classname + "$Reduce");
				return t.newInstance();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return null;
	}
	
}
