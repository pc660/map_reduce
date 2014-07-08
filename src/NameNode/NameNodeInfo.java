package NameNode;

import java.io.Serializable;

public class NameNodeInfo implements Serializable{
	public String hostname;
	public int port;
	public int factor;
	public int chunck_size;
}
