package DataNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.soap.Node;

import org.omg.CORBA.portable.InputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import Message.AckMessage;
import Message.AppendMessage;
import Message.DatanodeAckMessage;
import Message.DeleteFileMessage;
import Message.DownloadMessage;
import Message.DuplicateMessage;
import Message.FileMessage;
import Message.Message;
import Message.ReadMessage;
import Message.WriteMessage;
import file.Chunck;

public class DataNode {
	public DataNodeInfo nodeinfo;
	public String configure_file  = "dataNode.xml";
	public String checkpoint_file;
	//public Hashmap<String, DFSfile> file_map;
	ServerSocket upload_server;
	public HashMap<String, Chunck> chunck_map;
	public String namenode_host;
	public int namenode_port;
//	public String rootname;
	public DataNode (String filename)
	{

		//chunck_map = new  HashMap<String, Chunck>();
		this.configure_file = filename;
		File fXmlFile = new File(configure_file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			//upload_server = new ServerSocket(nodeinfo.upload_port);
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("DataNodeInfo");
			org.w3c.dom.Node node = nList.item(0);
			Element eElement = (Element) node;
			nodeinfo = new DataNodeInfo();
			nodeinfo.hostname = eElement.getElementsByTagName("host").item(0).getTextContent();
			nodeinfo.read_port = Integer.parseInt(eElement.getElementsByTagName("readport").item(0).getTextContent());
			nodeinfo.download_port = Integer.parseInt(eElement.getElementsByTagName("download").item(0).getTextContent());
			nodeinfo.upload_port = Integer.parseInt(eElement.getElementsByTagName("upload").item(0).getTextContent());
			nodeinfo.name_port = Integer.parseInt(eElement.getElementsByTagName("name").item(0).getTextContent());
			nodeinfo.rootdirectory = eElement.getElementsByTagName("root").item(0).getTextContent();
			nodeinfo.chunck_size = Integer.parseInt(eElement.getElementsByTagName("max_size").item(0).getTextContent());
			checkpoint_file = eElement.getElementsByTagName("checkpoint").item(0).getTextContent();
			nList = doc.getElementsByTagName("NameNode");
			node = nList.item(0);
			eElement = (Element) node;
			namenode_host = eElement.getElementsByTagName("hostname").item(0).getTextContent();
			namenode_port = Integer.parseInt(eElement.getElementsByTagName("port").item(0).getTextContent());
			//read checkpoint
			String chunck_map_name = doc.getElementsByTagName("chunck_map").item(0).getTextContent();
			File file = new File (chunck_map_name);
			if (file.exists())
			{

				FileInputStream in = new FileInputStream(chunck_map_name);
				ObjectInputStream input= new ObjectInputStream ( in );
				chunck_map = (HashMap<String, Chunck> )input.readObject();
			}
			else
			{
				chunck_map = new HashMap<String, Chunck>();
			}
			System.out.println("Finish initilzation and start serice");
			connectToNameaddress();
			//start service
			read_service read = new read_service(nodeinfo.read_port);
			write_service write = new write_service(nodeinfo.upload_port);
			read.start();
			write.start();
			heartbeat_service heart = new heartbeat_service(nodeinfo.name_port);
			heart.start();
			checkpoint_service check = new checkpoint_service();
			check.start();
			chunckservice chunck = new chunckservice (nodeinfo.download_port);
			chunck.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private class chunckservice extends Thread
	{
		public int port;
		ServerSocket listening;
		
		public chunckservice (int port) throws IOException
		{
			this.port = port;
			listening = new ServerSocket (this.port);
		}
		
		@Override
		public void run()
		{
			while(true)
			{
				Socket socket;
				try {
					socket = listening.accept();
					ObjectInputStream input = new ObjectInputStream (socket.getInputStream());
					Chunck f = (Chunck) input.readObject();
					
					
					
//					File file = new File ( nodeinfo.rootdirectory + "/" + f.chunckname  );
					BufferedReader in = new BufferedReader(new FileReader(nodeinfo.rootdirectory + "/" + f.chunckname));
					PrintWriter out = new PrintWriter(socket.getOutputStream());
					String line = "";
					while((line = in.readLine()) != null)
					{
						out.println(line);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}		
		}
	}
	
	//public
	public void connectToNameaddress()
	{
		try {
			Socket s = new Socket (namenode_host, namenode_port);
			 DatanodeAckMessage msg = new DatanodeAckMessage(nodeinfo);
			 msg.map = this.chunck_map;
			 ObjectOutputStream output = new ObjectOutputStream(   s.getOutputStream());
			 output.writeObject(msg);
			 //output.writeObject(obj);
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public boolean write (Chunck f, byte [] buffer)
	{
	/*	Socket socket = upload_server.accept();
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		File file = new File ( "root/" + f.chunckname  );
		PrintWriter out = new PrintWriter(new FileOutputStream(file),true);
		*/
		
		
		
		return false;
	}
	public synchronized void delete(String chunckname)
	{
		String filename = chunckname;
		
		
		if (chunck_map.containsKey(filename))
			chunck_map.remove(filename);
		//write log
		
		File file = new File(nodeinfo.rootdirectory +"/" + filename);
		if (file.isFile())
			file.delete();
	}
	
	public  synchronized  boolean  uploadChunck (Chunck f)
	{
		try {
			Socket socket = upload_server.accept();
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			File file = new File ( nodeinfo.rootdirectory + "/" + f.chunckname  );
			PrintWriter out = new PrintWriter(new FileOutputStream(file),true);
			String line = "";
			while((line = in.readLine()) != null)
			{
				out.println(line);
				//out.flush();
			}
			socket.close();
			in.close();
			out.close();
			return true;
		} catch (IOException e) {
			return false;
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}

		
		//return false;
	}
	

	
	private class heartbeat_service extends Thread
	{
		ServerSocket heartbeat_listening;
		public int port; 
		public heartbeat_service( int port)
		{
			try {
				this.heartbeat_listening = new ServerSocket (port);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public void run()
		{
			while(true)
			{
				try {
					Socket ss = heartbeat_listening.accept();
					 BufferedReader in = new BufferedReader( new InputStreamReader(ss.getInputStream()));
					 String line = "";
					 //	 System.out.println("Accepted");
					 while (  (line = in.readLine())!=null )
					 {
						 //	 System.out.println(line);
						 if (line.equals("Alive"))
							 break;
					 }
					 
					 ObjectOutputStream output = new ObjectOutputStream(   ss.getOutputStream());
					 // System.out.println(chunck_map.size() );
					 synchronized (chunck_map){
						 output.writeObject(chunck_map);
						 output.flush();
					 }
					 
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
	private class checkpoint_service extends Thread
	{
		public checkpoint_service()
		{
			
		}
		@Override 
		public void run()
		{
			
			FileOutputStream out;
			try {
				File old = new File(nodeinfo.rootdirectory + "/" + checkpoint_file);
				if (old.exists()){
					
				if (old.isFile()) {
					old.renameTo(new File(nodeinfo.rootdirectory + "/" + checkpoint_file + ".old"));
				}
				
				out = new FileOutputStream(checkpoint_file);
				ObjectOutputStream output = new ObjectOutputStream (out);
				synchronized(chunck_map){
					output.writeObject(chunck_map);
					output.flush();
				}
				}
				else
				{
					out = new FileOutputStream(checkpoint_file);
					ObjectOutputStream output = new ObjectOutputStream (out);
					synchronized(chunck_map){
						output.writeObject(chunck_map);
						output.flush();
				}
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	
	private class write_service extends Thread
	{
		ServerSocket write_service;
		int write_port;
		public write_service(int port)
		{
			this.write_port = port;
			try {
				write_service = new ServerSocket (this.write_port);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override 
		public void run()
		{
			while (true)
			{
				try {
					Socket ss = write_service.accept();
					ObjectInputStream input = new ObjectInputStream ( ss.getInputStream());
					Message m = (Message)input.readObject();
					if (m instanceof AppendMessage)
					{
						append_handler handler = new append_handler (ss, (AppendMessage)m);
						handler.start();
					}
					else if (m instanceof WriteMessage)
					{
						System.out.println("Debug: write message received");
						write_handler handler = new write_handler (ss, (WriteMessage) m);
						handler.start();
					}
					else if (m instanceof DuplicateMessage)
					{
						System.out.println("Debug: Duplicate message received");
						DuplicateMessage msg = (DuplicateMessage)m;
						duplicate_service handler = new duplicate_service( (DuplicateMessage)msg );
						handler.start();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
	private class duplicate_service extends Thread
	{
		DuplicateMessage m;
		public duplicate_service( DuplicateMessage m)
		{
			this.m = m;
		}
		@Override
		public void run()
		{
			File file = new File (nodeinfo.rootdirectory + "/" + m.chunck_name);
			
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				ArrayList<String> text = new ArrayList<String> ();
				
				String line;
				while ((line = br.readLine()) != null) {
				   text.add(line);
				}
				WriteMessage msg = new WriteMessage(m.chunck_name, text);
				for (DataNodeInfo d : m.node_list)
				{
					Socket s = new Socket (d.hostname, d.upload_port);
					ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
					output.writeObject(msg);
				}
				
				
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}
	//download_the_whole_file
	private class read_service extends Thread
	{
		ServerSocket read_service;
		int download_port;
		public read_service(int port)
		{
			this.download_port = port;
			try {
				read_service = new ServerSocket (this.download_port);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override
		public void run()
		{
			while (true)
			{
				try {
					Socket ss = read_service.accept();
					ObjectInputStream input = new ObjectInputStream ( ss.getInputStream());
					Message m = (Message)input.readObject();
					/*if (m instanceof DownloadMessage)
					{
						download_handler handle = new download_handler (ss,((DownloadMessage) m).ChunckName );
						handle.start();
					}*/
					if (m instanceof ReadMessage)
					{
						read_handler handle = new read_handler (ss,(ReadMessage) m);
						handle.start();
					}
					else if (m instanceof DeleteFileMessage)
					{
						DeleteFileMessage msg = (DeleteFileMessage) m;
						String chunck_name = msg.filename();
						delete(chunck_name);	
					}
					
					
					//download_handler download = new download_handler (ss);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
	private class read_handler extends Thread
	{
		Socket socket ; 
		ReadMessage m ; 
		public read_handler (Socket s, ReadMessage m)
		{
			this.socket = s;
			this.m = m;
		}
		
		@Override 
		public void run()
		{
			try {
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				//BufferedReader file = new BufferedReader(new FileReader("root/" + m.ChunckName));
				String line = "";
				//RandomAccessFile file = new RandomAccessFile ("root/" + m.ChunckName, "rws");
				File file= new File (nodeinfo.rootdirectory + "/" + m.ChunckName);
				BufferedReader br = new BufferedReader(new FileReader(file));
				//String line;
				int count = 0;
				ArrayList<String> text = new ArrayList<String> ();
				while ((line = br.readLine()) != null) {
					
					if (count >= m.start && count <= m.end)
					{
						text.add(line);
					}
					// process the line.
				}
				
				//file.seek(m.offset);
				//byte [] buffer = new byte [m.length];
				//file.read(buffer, m.offset, m.length);
				FileMessage msg = new FileMessage ( m.ChunckName, text);
				out.writeObject(msg);
				out.flush();
				//out.write(buffer);
				//String s = "";
				//out.write(buffer);
				//out.flush();
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}
	private class download_handler extends Thread
	{
		Socket socket;
		String chunck_name;
		public download_handler(Socket s, String chunck_name)
		{
			this.socket = s;
			this.chunck_name = chunck_name;
		}
		
		@Override
		public void run()
		{
			try {
				//BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				//DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				//FileInputStream is = new FileInputStream("root/" + chunck_name);
				File file= new File (nodeinfo.rootdirectory+ "/" + chunck_name);
				
				BufferedReader br = new BufferedReader (new FileReader(file)   );
				ArrayList<String> text = new ArrayList<String> ();
				String line = "";
				while ((line = br.readLine()) != null) {
					text.add(line);
				}
				//int count = is.available();
				//byte[] bs = new byte[count];
				//file.read(bs);
				FileMessage msg = new FileMessage ( chunck_name, text);
				out.writeObject(msg);
				out.flush();
				br.close();
				socket.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private class write_handler extends Thread
	{
		Socket socket;
		WriteMessage m ;
		public write_handler ( Socket s , WriteMessage m)
		{
			this.socket = s;
			this.m = m;
		}
		@Override
		public void run()
		{
			try {
				FileWriter out = new FileWriter(nodeinfo.rootdirectory + "/"+m.chunck_name);
				for(String line : m.text)
				{
					out.write(line + '\n');
				}
				Chunck chunck = new Chunck(nodeinfo);
				
				chunck.chunckname = m.chunck_name;
				
				synchronized(chunck_map)
				{
					chunck_map.put(m.chunck_name, chunck);
				}
				out.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private class append_handler extends Thread
	{
		Socket socket;
		AppendMessage m;
		public append_handler (Socket s, AppendMessage m)
		{
			this.socket = s;
			this.m = m;
		}
		@Override
		public void run()
		{
			try {
				//FileOutputStream out = new FileOutputStream(m.chunck_name, true);
				FileWriter out = new FileWriter(nodeinfo.rootdirectory + "/" + m.chunck_name,true);
				BufferedWriter output = new BufferedWriter(out);
				for (String line :m.text)
				{
					output.append(line+ "\n");
				}
				
				//out.write(m.buffer);
//				out.flush();
//				out.close();
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
