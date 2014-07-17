package Communication;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import Message.AckMessage;
import Message.CreateChunckMessage;
import Message.CreateMessage;
import Message.DeleteFileMessage;
import Message.DisplayMessage;
import Message.FileMessage;
import Message.GetChunckMessage;
import Message.GetMessage;
import Message.ReadMessage;
import Message.WriteMessage;
import file.Chunck;
import file.DFSfile;

public class Comm {
	
	public String NameNode_address;
	public int NameNode_read_port;
	public int NameNode_write_port;
	public int max_size;
	public DFSfile getFile (String filename) throws UnknownHostException, IOException, ClassNotFoundException
	{
		System.out.println(NameNode_address);
		Socket s = new Socket (NameNode_address, NameNode_read_port);
		GetMessage m = new GetMessage(filename);
		ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
		output.writeObject(m);
		output.flush();
		
		ObjectInputStream input = new ObjectInputStream (s.getInputStream());
		DFSfile file = (DFSfile) input.readObject();
		return file;	
	}
	
	public ArrayList<String> catFile (String filename) throws UnknownHostException, IOException, ClassNotFoundException
	{
		Socket s = new Socket (NameNode_address, NameNode_read_port);
//		ReadMessage
		//ReadMessage m = new ReadMessage ()
		//Socket s = new Socket (NameNode_address, NameNode_read_port);
		GetMessage m = new GetMessage(filename);
		ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
		output.writeObject(m);
		output.flush();
		
		ObjectInputStream input = new ObjectInputStream (s.getInputStream());
		DFSfile file = (DFSfile) input.readObject();
		ArrayList<String>  list = new ArrayList<String> ();
		
		System.out.println(file.filename);
		for (int i = 0; i< file.chuncklist.size(); i++)
		{
		//	byte [] buffer = readChunck (file.chuncklist.get(i).)
			/**
			 * Connect to the latest chunck based on information, now just get the first one
			 */
			
			System.out.println(file.chuncklist.get(i).get(0).chunckname);
			ArrayList<String> text  = readChunck (file.chuncklist.get(i).get(0), 0, max_size);
			for (String line : text)
			{
				list.add(line);
			//	System.out.println(line);
			}
		
		}
		return list;
	}
	
	public ArrayList<String> readChunck (Chunck file,int offset, int length) throws IOException, ClassNotFoundException
	{
		
		Socket s = new Socket (file.nodeInfo.hostname, file.nodeInfo.read_port);
		ReadMessage m = new ReadMessage (file.chunckname, offset, length);
		ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
		output.writeObject(m);
		output.flush();
		ObjectInputStream input = new ObjectInputStream(  s.getInputStream() );
		FileMessage msg = (FileMessage)input.readObject();
		return msg.text;
		
	}
/*	private boolean check_file_available (String filename)
	{
		
	}*/
	public DFSfile createFile (String filename) throws ClassNotFoundException
	{
		CreateMessage msg = new CreateMessage(filename);
		Socket s;
		DFSfile file = null;
		try
		{
			s = new Socket (NameNode_address, NameNode_read_port);
			
			ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
			output.writeObject(msg);
			output.flush();
			
			
			System.out.println("Finish writing");
			ObjectInputStream input = new ObjectInputStream (s.getInputStream());
			file = (DFSfile) input.readObject();
			System.out.println("Finish reading");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return file;
		
	}
	
	
	public DFSfile getNewFile (String filename) throws UnknownHostException, IOException, ClassNotFoundException
	{
		removeFile(filename);
		DFSfile file  = getFile(filename);
		if (file == null)
		{
			System.out.println("Do not have the file, try to create one");
			file = createFile (filename);
			System.out.println("finish creating the file");
		}
		if (file == null)
		{
			System.out.println("Create Failed, wiered");
		}
		
		
		
		
		return file;
	//	upload_chunck ()
	}
	public boolean uploadChunck (ArrayList<String> text, Chunck chunck)
	{
		//NameNodeInfo = chunck.nodeInfo;
		String hostname = chunck.gethost();
		int port = chunck.getuploadport();
		try {
			Socket s = new Socket (hostname, port);
			String chunck_name = chunck.chunckname;
			
			WriteMessage msg = new WriteMessage( chunck_name,text);
			System.out.println(msg.chunck_name);
			ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
			
			output.writeObject(msg);
			output.flush();
			
			
			return true;
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return false;
		}
		
		
	//	return false;
	}
	public Chunck getNextChunck(DFSfile file)
	{
		CreateChunckMessage msg = new CreateChunckMessage (file.filename);
		Socket s;
		try {
			s = new Socket (NameNode_address, NameNode_read_port);
			ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
			output.writeObject(msg);
			output.flush();
			
			ObjectInputStream input = new ObjectInputStream (s.getInputStream());
			Chunck chunck_file = (Chunck) input.readObject();
			return chunck_file;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
		
	}
	public ArrayList<String> ls()
	{
		DisplayMessage msg = new DisplayMessage();
		try {
			Socket s = new Socket (NameNode_address, NameNode_read_port);
			ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
			output.writeObject(msg);
			ObjectInputStream input = new ObjectInputStream (s.getInputStream());
			ArrayList<String> file_list = (ArrayList<String>) input.readObject();
			return file_list;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public ArrayList<String> readfile (String filename, int start, int end)
	{
		if (start < 0 || start > end)
		{
			//handle exception
			
			return null;
		}
		else 
		{
		
			DFSfile file;
			try {
				file = this.getFile(filename);
				int end_chunck = end / max_size;
				int end_offset = max_size;
				
				
				int start_chunck = start / max_size;
				int start_offset = start % max_size;
				ArrayList<String> result = new ArrayList<String> ();
				while (start_chunck <= end_chunck)
				{
					if (start_chunck == end_chunck)
					{
						if (end != -1)
							end_offset = end % max_size;
					}
					ArrayList<String> tmp = readChunck(file.chuncklist.get(start_chunck).get(0), start_offset, end_offset);
					result.addAll(tmp);
					start_chunck ++ ;
				}
			return result;
				
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			 
		}
		return null;
		
		
	}
	public boolean removeFile(String filename)
	{
		try {
			//Socket s = new Socket (NameNode_address, NameNode_read_port);
			DFSfile file = getFile (filename);
			if (file != null)
			{
				for (int i = 0; i< file.chuncklist.size() ; i++)
				{
					for (int j = 0; j< file.chuncklist.get(i).size();j++)
					{
						String chunck_name = filename + "_chunck"+i;
						Socket s = new Socket (file.chuncklist.get(i).get(j).nodeInfo.hostname,file.chuncklist.get(i).get(j).nodeInfo.read_port );
						DeleteFileMessage msg = new DeleteFileMessage (chunck_name);
						ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
						output.writeObject(msg);
						
						
					}
				}
				
				
				DeleteFileMessage msg = new DeleteFileMessage (filename);
				Socket s = new Socket (NameNode_address, NameNode_read_port);
				ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
				output.writeObject(msg);
				ObjectInputStream input = new ObjectInputStream (s.getInputStream());
				AckMessage m = (AckMessage) input.readObject();
				if (m.finished)
					return true;
				else
				{
					System.out.println(m.error_code);
					return false;
				}
			}
			else 
			{
				//throw exception
				System.out.println("file not found");
				return false;
			}
			
			
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
		
	}
	
	
	
//	public void upload_chunck ()
	
	
	
	
	
}
