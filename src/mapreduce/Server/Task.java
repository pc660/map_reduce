package mapreduce.Server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import ding.*;
import File_system.ChunckReader;
import File_system.DistributedFileSystem;


/*
 * class for running tasks
 * 
 * */
public class Task {
	private Taskstatus taskStatus;
	private Taskconfig taskConfig;
	
	/*
	 * first get mapper class
	 * read file line by line
	 * 
	 * 
	 * */
	
	public Task(Taskconfig taskConfig) {
		this.taskConfig = taskConfig;
		taskStatus = new Taskstatus();
		taskStatus.state = Status.Runnable;
		taskStatus.log = new ArrayList<String>();
	}
	
	public void runTask() {
		String jobType = taskConfig.jobtype.toLowerCase();
		if (jobType.equals("map")) {
			runMapTask();
			taskStatus.state = Status.Running;
		} else if (jobType.equals("reduce")) {
			runReduceTask();
			taskStatus.state = Status.Running;
		}
		taskStatus.state = Status.Fail;
		taskStatus.log.add("Can not runTask()");
	}
	
	private void runMapTask() {
		try {
			do_mapper().start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			taskStatus.state = Status.Fail;
			taskStatus.log.add("Failed to start do_mapper() thread");
			return;
		}
		
	}
	
	private void runReduceTask() {
		do_reducer().start();
	}
	
	public Thread do_mapper () throws IOException
	{
		return new Thread ( 
				new Runnable ()
				{	
					MapRecordReader rr = new MapRecordReader(taskConfig.mapinput, taskStatus.log);

					public void run()
					{	
						
						//System.out.println(taskConfig.mapinput.chunckname);
						if (!rr.init()) {
							taskStatus.state = Status.Fail;
							taskStatus.log.add("Failed during init MapRecordReader");
							return;
						}
						Mapper mapper =  (Mapper) taskConfig.getMapper();
						String filename = "job"+ taskConfig.jobID + "_" + "task" + 
								taskConfig.taskID;
						MapperOutputCollector out = new MapperOutputCollector(  taskConfig.mapinput, taskConfig.numOfRed);
						String nextLine = null;
						try {
							//System.out.println(rr.nextKeyVlaue());
							while (rr.nextKeyVlaue()) {
								//System.out.println(rr.getCurrentKey());
								//System.out.println(rr.getCurrentValue());
								//System.out.println(mapper.getClass().toString());
								mapper.map(rr.getCurrentKey(), rr.getCurrentValue(), out);
								
							}
							
						} catch (IOException e) {
							taskStatus.state = Status.Fail;
							taskStatus.log.add("Failed during input K/V to user's mapper");
							return;
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						out.close();
						taskStatus.state = Status.Succeed;
					}
				}
			);
		/*
		 * read file line by line 
		 * and then call outputcollector
		 * */
		
	}
	
	public Thread do_reducer ()
	{
		
		return new Thread ( 
				
				new Runnable ()
				{
					public void run()
					{
						
					}
				/*	ArrayList<String>  localFileNames = new ArrayList<String>();
					public void run()
					{
						for (String filename : taskConfig.inputfile) {
							localFileNames.add(Tools.downloadDFSToLocal(filename));
						}
						
						String basepath = localFileNames.get(0);
						for (int i = 1; i < localFileNames.size(); i++) {
							String mergedFileName = taskConfig.jobID+ "_" + taskConfig.taskID + "_" + i;
							basepath = Tools.mergeSortedFiles(basepath, localFileNames.get(i), mergedFileName);
						}
						String mergedFilePath = basepath;
						ReduceRecordReader rr = new ReduceRecordReader(mergedFilePath);
						String resultFileName = "job" + taskConfig.jobID + "_result_" + "part"+ taskConfig.taskID;
						RedecerOutputCollector out = new RedecerOutputCollector(resultFileName);
						
						ArrayList<String> list = new ArrayList<String>();
						String key = null;
						String value = null;
						while(rr.nextKeyVlaue()) {
							String newKey = rr.getCurrentKey();
							value = rr.getCurrentValue();
							if (key == null || newKey.equals(key)) {
								list.add(value);
								key = newKey;
							} else {
								reduce.reduce(key, list.iterator(), out);
								key = newKey;
								list = new ArrayList<String>();
								list.add(value);
							}	
						}
						// do the last key values
						if (list.size() > 0) {
							reducer.reduce(key, list.iterator(), out);
						}
						
						out.close();
						
						//Upload results to DFS;
						DistributedFileSystem dfs = new DistributedFileSystem();
						dfs.uploadFile(resultFileName);
						
						//deldete local files
						File resultFile = new File(resultFileName);
						if (resultFile.exists() && resultFile.isFile()) {
							resultFile.delete();
						}
						resultFile = null;
						taskStatus.state = Status.Succeed;
					}*/
				}
			);
		
		
	}
}
