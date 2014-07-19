import java.io.BufferedReader;

import example.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import mapreduce.JobClient;
import mapreduce.Jobconfig;
import mapreduce.Server.Jobtracker;
import mapreduce.Server.Tasktracker;
import NameNode.NameNode;
import DataNode.DataNode;
import File_system.DistributedFileSystem;
public class main {
	
	public static void main (String [] args) throws IOException
	{
		System.out.println(args[0]);
		if (args[0].equals("namenode"))
		{
			System.out.println("namenode");
			NameNode master = new NameNode();
		}
		else if (args[0].equals("dfs"))
		{
			DistributedFileSystem dfs = new DistributedFileSystem();
			while(true)
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String command = "";
				command = br.readLine();
				String [] new_args = command.split(" ");
				if (new_args[0].equals("upload"))
				{
					dfs.uploadFile(new_args[1]);
				}
				else if (new_args[0].equals("cat"))
				{
					dfs.catFile(new_args[1]);
				}
				else if (new_args[0].equals("ls"))
				{
					ArrayList<String> list = dfs.ls();
					System.out.println("File List: ");
					for(String s : list)
					{
						System.out.println(s);
					}
				}
				else if (new_args[0].equals("download"))
				{
					dfs.DownloadFile(new_args[1], new_args[2]);
				}
				else if (new_args[0].equals("show"))
				{
					dfs.showFileInfo(new_args[1]);
				}
				else if (new_args[0].equals("remove"))
				{
					dfs.removeFile(new_args[1]);
				}
				else if (new_args[0].equals("read"))
				{
					ArrayList<String> text ;
					if (new_args.length == 3)
					{
						text = dfs.readFile(new_args[1], Integer.parseInt(new_args[2]));
					}
					else 
						text = dfs.readFile(new_args[1], Integer.parseInt(new_args[2]), Integer.parseInt(new_args[3]));
					for (String str : text)
						System.out.println(str);
				}
				
				
			}
			
			
			
		}
		else if (args[0].equals("datanode"))
		{
			DataNode slave = new DataNode(args[1]);
			
			System.out.println("datanode");
		}
		else if (args[0].equals("random"))
		{
			Random random = new Random();
			File file = new File(args[1]);
			FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bufferWriter = new BufferedWriter(fileWriter);

			int i = 0;
			
			while (i++ < 9000) {
				bufferWriter.write("line: " + i + " ");
				bufferWriter.write(' ');
				bufferWriter.write((char) (random.nextInt(94) + 33));
				bufferWriter.write(' ');
				bufferWriter.write((char) (random.nextInt(94) + 33));
				bufferWriter.write(' ');
				bufferWriter.write((char) (random.nextInt(94) + 33));
				bufferWriter.write(' ');
				bufferWriter.write((char) (random.nextInt(94) + 33));
				bufferWriter.write(' ');
				bufferWriter.write((char) (random.nextInt(94) + 33));
				bufferWriter.write(' ');
				bufferWriter.write((char) (random.nextInt(94) + 33));
				bufferWriter.write('\n');
			}
			bufferWriter.close();
		}
		else if (args[0].equals("jobtracker"))
		{
			Jobtracker tracker = new Jobtracker();
			
			
		}
		else if (args[0].equals("example.wordcount"))
		{
			Jobconfig config = new Jobconfig();
			config.jobName = "wordcount";
			
			
			config.mapInputKeyClass = String.class;
			config.mapInputValueClass = String.class;
			config.mapOutputKeyClass = String.class;
			config.mapOutputValueClass = String.class;
			//wordcount t= new wordcount();
			//config.jobClass = wordcount.class;
			//config.mapClass = wordcount.Map.class;
			//config.reduceClass = wordcount.reduce.class;
			config.reduceInputKeyClass = String.class;
			config.reduceInputValueClass = String.class;
			config.reduceOutputKeyClass = String.class;
			config.reduceOutputValueClass = String.class;
			//config.mapInputPath = "1";
			//config.setJar("bin/example/wordcount.class");
			config.filename = args[1];
			config.setJar("bin/example/wordcount.class");
			config.classname = "example.wordcount";
			//String s= "bin/example/wordcount.class";
			
			//System.out.println(s.substring(0, s.length() - 6));
			JobClient client = new JobClient();
			client.runJob(config);
			

		}
		else if (args[0].equals("tasktracker"))
		{
			int port = Integer.parseInt(args[1]);
			Tasktracker task = new Tasktracker(port);
			
			
			
		}
		
		
	}
	
	

}
