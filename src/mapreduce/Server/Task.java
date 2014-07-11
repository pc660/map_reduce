package mapreduce.Server;


/*
 * class for running tasks
 * 
 * */
public class Task {
	
	Taskstatus config;
	/*
	 * first get mapper class
	 * read file line by line
	 * 
	 * 
	 * */
	
	public Thread do_mapper ()
	{
		return new Thread ( 
				new Runnable ()
				{
					public void run()
					{
						
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
