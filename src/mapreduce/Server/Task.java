package mapreduce.Server;

import java.io.IOException;

import ding.Mapper;
import ding.MapperOutputCollector;
import File_system.ChunckReader;


/*
 * class for running tasks
 * 
 * */
public class Task {
	private TaskStatus taskStatus;
	private TaskConfig taskConfig;
	
	/*
	 * first get mapper class
	 * read file line by line
	 * 
	 * 
	 * */
	
	public Task(TaskConfig taskConfig) {
		this.taskConfig = taskConfig;
		
	}
	
	public void runTask() {
		switch (taskConfig.getJobType()) {
			case MAP: 
				runMapTask();
				break;
			case REDUCE:
				runReduceTask();
				break;
		}
	}
	
	private void runMapTask() {
		do_mapper().start();
		
	}
	
	private void runReduceTask() {
		do_reducer().start();
	}
	
	public Thread do_mapper () throws IOException
	{
		return new Thread ( 
				new Runnable ()
				{	
					ChunckReader chunckReader = new ChunckReader(taskConfig.getChunck());
					public void run()
					{	
						Mapper mapper =  taskConfig.getMapper();
						String filename = "job"+ taskConfig.getJobID() + "_" + "task" + 
								taskConfig.getTaskID();
						MapperOutputCollector out = new MapperOutputCollector(filename, taskConfig.getNumOfRed());
						String nextLine = null;
						try {
							while ((nextLine = chunckReader.readline()) != null) {
								mapper.map(key, nextLine, out);
							}
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						out.close();
						;
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
				}
			);
		
		
	}
}
