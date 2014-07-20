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
	public Task (Taskstatus status)
	{
		this.taskConfig = status.config;
		this.taskStatus = status;
		
	}
	
	public void runTask() {
		String jobType = taskConfig.jobtype.toLowerCase();
		if (jobType.equals("map")) {
			taskStatus.state = Status.Running;
			runMapTask();
			
		} else if (jobType.equals("reduce")) {
			taskStatus.state = Status.Running;
			runReduceTask();
			
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
						taskStatus.state = Status.Running;
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
						ArrayList<String>  localFileNames = new ArrayList<String>();
						taskStatus.state = Status.Running;
						Reducer reducer =  (Reducer) taskConfig.getReducer();
						String reduce_name = "job" + taskConfig.jobID + "_reduce" + taskConfig.taskID;
						
						System.out.println(reduce_name);
						for (String str : taskConfig.inputfile.get(reduce_name)   ) {
						//	System.out.println(taskConfig.inputfile.get(str));
							String path = Tools.downloadDFSToLocal(str, taskStatus.log);
							
							if (path == null) {
								taskStatus.state = Status.Fail;
								taskStatus.log.add("Failed during Tools.downloadDFSToLocal");
								return;
							}
							//System.out.println("path: " + path);
							localFileNames.add(path);
						}
						
						String basepath = localFileNames.get(0);
						System.out.println("basepath: " + basepath);
						
						
						
						
						for (int i = 1; i < localFileNames.size(); i++) {
							
							String mergedFileName = taskConfig.jobID+ "_" + taskConfig.taskID + "_" + i;
							System.out.println("This is the merge" + i);
							basepath = Tools.mergeSortedFiles(basepath, localFileNames.get(i), mergedFileName,taskStatus.log);
							if (basepath == null) {
								taskStatus.state = Status.Fail;
								
								System.out.println("Failed during Tools.mergeSortedFiles");
								taskStatus.log.add("Failed during Tools.mergeSortedFiles");
								return;
							}
						}
						
						
							System.out.println("finish merge now!");
							String mergedFilePath = basepath;
						
							ReduceRecordReader rr = new ReduceRecordReader(mergedFilePath, taskStatus.log);
							if (!rr.init()) {
								taskStatus.state = Status.Fail;
								System.out.println("Failed during init.ReduceRecordReader");
								taskStatus.log.add("Failed during init ReduceRecordReader");
								return;
							}
							
							String resultFileName =  "job" + taskConfig.jobID + "_result_" + "part"+ taskConfig.taskID;
							RedecerOutputCollector out = new RedecerOutputCollector("data/"+ resultFileName, taskStatus.log);
							if (!out.init()) {
								taskStatus.state = Status.Fail;
								System.out.println("Failed during init.RedecerOutputCollector");
								taskStatus.log.add("Failed during init RedecerOutputCollector");
								return;
							}
							
							ArrayList<String> list = new ArrayList<String>();
							String key = null;
							String value = null;
							System.out.println("now start working");
							try {
								while(rr.nextKeyVlaue()) {
									String newKey = rr.getCurrentKey();
									value = rr.getCurrentValue();
									//System.out.println("one reducer");
									//System.out.println(list.size());
									if (key == null || newKey.equals(key)) {
										list.add(value);
										
										key = newKey;
									} else {
										//System.out.println(key);
										//System.out.println(newKey);
										reducer.reduce(key, list.iterator(), out);
										key = newKey;
										list = new ArrayList<String>();
										list.add(value);
									}	
								}

								// do the last key values
								
								if (list.size() > 0) {
									System.out.println("do reduce" + taskStatus.jobId + "_reduce" + taskStatus.taskId);
									reducer.reduce(key, list.iterator(), out);
									System.out.println("finish reduce" + taskStatus.jobId + "_reduce" + taskStatus.taskId);
								}

							} catch (Exception e) {
								// TODO Auto-generated catch block
								taskStatus.state = Status.Fail;
								taskStatus.log.add("Failed during input K/V pair to reduce");
								e.printStackTrace();
								return;
							}
							
							out.close();
							//System.out.println("finish reduce" + taskStatus.jobId + "_reduce" + taskStatus.taskId);
							//Upload results to DFS;
							DistributedFileSystem dfs = new DistributedFileSystem();
							System.out.println("**************upload" + resultFileName);
							dfs.uploadFile(resultFileName);
							System.out.println("*************Finish uploading");
							//deldete local files
							File resultFile = new File(resultFileName);
							if (resultFile.exists() && resultFile.isFile()) {
								resultFile.delete();
							}
							resultFile = null;
							taskStatus.state = Status.Succeed;
						
						}
					


				}
				);

		
	}
}
