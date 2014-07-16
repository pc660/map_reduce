package File_system;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import file.Chunck;


/*
 * Some optimization for reading chunck,
 * only used in map reduce 
 * 
 * */
public class ChunckReader {
	Chunck chunck; 
	Socket readsocket;
	BufferedReader in;
	PrintWriter out;
	boolean local ;
	public ChunckReader (String filename) throws FileNotFoundException
	{
		File file = new File(chunck.nodeInfo.rootdirectory + "/" + chunck.chunckname);
		if(file.exists())
		{
				in = new BufferedReader (  new FileReader (  chunck.nodeInfo.rootdirectory + "/" + chunck.chunckname ) );
				local = true;
		}
		else
		{
			//try to get file from server
			//String [] args = filename.split("_");
		
			
			
			
		}
	}
	public ChunckReader(Chunck chunck) throws UnknownHostException, IOException
	{
		this.chunck = chunck;
		
		File file = new File(chunck.nodeInfo.rootdirectory + "/" + chunck.chunckname);
		if(file.exists())
		{
				in = new BufferedReader (  new FileReader (  chunck.nodeInfo.rootdirectory + "/" + chunck.chunckname ) );
				local = true;
		}
		else{
			local = false;			
				readsocket = new Socket (chunck.nodeInfo.hostname, chunck.nodeInfo.download_port);
				ObjectOutputStream output = new ObjectOutputStream (readsocket.getOutputStream());
				output.writeObject(chunck);
				in = new BufferedReader( new InputStreamReader (readsocket.getInputStream()) );
				
		}
		
	}
	public String readline() throws IOException
	{
		return in.readLine();

	}
	
}
