package mapreduce.Server;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import file.Chunck;

public class TaskConfig implements Serializable {

	private int jobID;
	private int taskID;
	public enum JobType {MAP, REDUCE};
	private JobType type;
	private Chunck chunck;
	private int numOfRed;
	private byte[] jar;
	private HashMap<String, String> Inputfile;
	private String jarPath;
	private JarFile jarFile;
	
	public TaskConfig(int jobID, int taskID, JobType type, Chunck chunck, String jarPath, int numOfRed, byte[] jar) {
		this.jobID = jobID;
		this.taskID = taskID;
		this.type = type;
		this.chunck = chunck;
		this.jarPath = jarPath;
		this.numOfRed = numOfRed;
		this.jar = jar;
	}
	
	public int getJobID() {
		return this.jobID;
	}
	
	public int getTaskID() {
		return this.taskID;
	}
	
	public JobType getJobType() {
		return this.type;
	}
	
	public Chunck getChunck() {
		return this.chunck;
	}
	
	public String getJarPath() {
		return this.jarPath;
	}
	
	public int getNumOfRed() {
		return this.numOfRed;
	}
	
	public byte[] getJar() {
		return this.jar;
	}
}
