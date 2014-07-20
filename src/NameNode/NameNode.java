package NameNode;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import file.Chunck;
import file.DFSfile;
import DataNode.DataNodeInfo;
import Message.AckMessage;
import Message.CreateChunckMessage;
import Message.CreateMessage;
import Message.DatanodeAckMessage;
import Message.DeleteFileMessage;
import Message.DisplayMessage;
import Message.DuplicateMessage;
import Message.GetChunckMessage;
import Message.GetMessage;
import Message.Message;

public class NameNode {
	public NameNodeInfo nodeInfo;
	public ServerSocket server;
	public ArrayList<DataNodeInfo> dataNodeMap;
	//public int factor;
	public HashMap<String, DFSfile> filemap;
	public HashMap<DataNodeInfo, Integer> StorageMap;
	public String configure_file = "NameNode.xml";
	public NameNode ()
	{
		
		File fXmlFile = new File(configure_file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		//System.out.println("123");
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("NameNodeInfo");
			//System.out.println(nList.getLength());
			
			Node node = nList.item(0);
			Element eElement = (Element) node;
			nodeInfo = new NameNodeInfo();
			nodeInfo.port = Integer.parseInt(eElement.getElementsByTagName("port").item(0).getTextContent());
			nodeInfo.factor = Integer.parseInt( eElement.getElementsByTagName("replica_factor").item(0).getTextContent());
//			System.out.println("123");
			server = new ServerSocket (nodeInfo.port);
			//System.out.println("123");
			String hostname = server.getInetAddress().toString();
			if (hostname.contains("0.0.0.0"))
				nodeInfo.hostname = "127.0.0.1";
			else nodeInfo.hostname = hostname;
			//System.out.println(hostname);
			//read checkpoing 
			
			nList = doc.getElementsByTagName("dataNodeMap");
			String nodemap_name =  nList.item(0).getTextContent();
			//System.out.println("123");
			
			File file = new File (nodemap_name);
			if (file.exists()){
				FileInputStream in = new FileInputStream(nodemap_name);
			//	System.out.println("nodemap");
				ObjectInputStream input= new ObjectInputStream ( in );
				dataNodeMap = (ArrayList<DataNodeInfo> )input.readObject();
			}
			else
			{
				dataNodeMap = new ArrayList<DataNodeInfo>();
			}
			
			nList = doc.getElementsByTagName("filemap");
			String filemap_name =  nList.item(0).getTextContent();
			
			file = new File (filemap_name);
			if (file.exists()){
				FileInputStream in = new FileInputStream(filemap_name);
			//	System.out.println("filemap");
				ObjectInputStream input= new ObjectInputStream ( in );
				filemap = (HashMap<String, DFSfile> )input.readObject();
			}
			else
			{
				filemap = new HashMap<String, DFSfile>();
			}
			nList = doc.getElementsByTagName("StorageMap");
			String stroage_map_name =  nList.item(0).getTextContent();
//			System.out.println("123");
			file = new File(stroage_map_name);
			if (file.exists()){
				FileInputStream in = new FileInputStream(stroage_map_name);
				//System.out.println("storagemap");
				ObjectInputStream input= new ObjectInputStream ( in );
				StorageMap = ( HashMap<DataNodeInfo, Integer> )input.readObject();
			}
			else
			{
				//System.out.println("123");
				StorageMap = new HashMap<DataNodeInfo, Integer>();
			}
//			System.out.println("123");
			//start service now
		//	System.out.println("Finish  NameNode");
			start_service();
			//checkpoint check = new checkpoint();
			//check.start();
			
			heartBeat heart = new heartBeat();
			heart.start();
			
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
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
	
	public DFSfile createFile (String filename)
	{
		DFSfile file = new DFSfile ();
		//file.chuncklist = new ArrayList< ArrayList<Chunck > >();
		file.filename = filename;
		/*ArrayList< ArrayList<Chunck> > chunck_list = new ArrayList< ArrayList<Chunck> >();
		ArrayList<Chunck> tmp = new ArrayList<Chunck> ();
		for (int i = 0; i< list.size(); i++)
		{
			Chunck ch = new Chunck (file, list.get(i), "", false);
			tmp.add(ch);
		}
		chunck_list.add(tmp);
		file.chuncklist = chunck_list;*/
		filemap.put(filename, file);
		return file;
		
		
	}
	public DFSfile getFile(String filename)
	{
		if(filemap.containsKey(filename))
			return filemap.get(filename);
		else return null;
	}
	public void start_service()
	{
		/*hard code !! need to change later
		 * 
		 * */
		service ss = new service (server);
		ss.start();
	//	heartBeat hb = new heartBeat();
	//	hb.start();
		
		
	}
	private class service extends Thread 
	{
		ServerSocket ss;
		public service ( ServerSocket s)
		{
			this.ss = s;
		}
		@Override
		public void run()
		{
			while (true)
			{
				try {
					Socket socket = ss.accept();
				//	System.out.println("Receive one message");
					ObjectInputStream input = new ObjectInputStream (socket.getInputStream());
					Message m  = (Message) input.readObject();
				//	System.out.println(m.getClass().toString());
					if (m instanceof GetMessage)
					{
//						GetMessage 
						synchronized(filemap)
						{
							DFSfile file = getFile(m.getName() );
							
						//	System.out.println(m.getName());
							ObjectOutputStream output = new ObjectOutputStream (socket.getOutputStream());
							//output
							output.writeObject(file);
							output.flush();
						}
					}
					else if (m instanceof CreateChunckMessage)
					{
						//getchunckmessage
						CreateChunckMessage msg = (CreateChunckMessage)m;
						DFSfile file = filemap.get(msg.getfile());
						Chunck nextchunck = getNextChunck();
						nextchunck.file = file;
						synchronized(file){
							int id = getChunckId(file);
							nextchunck.id = id;
							nextchunck.chunckname = file.filename + "_chunck" + id;
							file.addChunck(nextchunck, id);
						}
						//	int id = getId(file);
						ObjectOutputStream output = new ObjectOutputStream (socket.getOutputStream());
						//output
						//System.out.println(nextchunck)
						output.writeObject(nextchunck);
						output.flush();	
					}
					else if (m instanceof CreateMessage)
					{
						CreateMessage msg = (CreateMessage)m;
						DFSfile file = createFile (msg.filename);
						ObjectOutputStream output = new ObjectOutputStream (socket.getOutputStream());
						
						output.writeObject(file);
						output.flush();	
					}
					else if (m instanceof DatanodeAckMessage)
					{
						addDataNode(((DatanodeAckMessage) m).info);
						
						updatefileInfo ( ((DatanodeAckMessage) m).map  );
					}
					else if (m instanceof DeleteFileMessage)
					{
						DeleteFileMessage msg = (DeleteFileMessage) m;
						synchronized (filemap)
						{
							if (filemap.containsKey(msg.filename())){
								filemap.remove(msg.filename());
								AckMessage ack = new AckMessage (true, "");
								ObjectOutputStream output = new ObjectOutputStream (socket.getOutputStream());
								output.writeObject(ack);
							}
							else
							{
								AckMessage ack = new AckMessage (false, "File not found");
								ObjectOutputStream output = new ObjectOutputStream (socket.getOutputStream());
								output.writeObject(ack);
							}
						}
					}
					else if (m instanceof DisplayMessage)
					{
						ls(socket);
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
	public void updatefileInfo(HashMap<String, Chunck> map)
	{
		for (String str : map.keySet())
		{
			String [] args = str.split("/");
			String [] args2 = str.split("_");
			String filename = args[0];
			if (filemap.containsKey(filename))
			{
				DFSfile file = filemap.get(filename);
				Chunck chunck = map.get(str);
				int id = chunck.id;
				if (id < file.chuncklist.size())
				{
					boolean judge = false;
					ArrayList<Chunck> list = file.chuncklist.get(id);
					for (Chunck tmp : list)
					{
						if (tmp.nodeInfo.hostname.equals(chunck.nodeInfo.hostname) && (tmp.nodeInfo.download_port == chunck.nodeInfo.download_port))
						{
							judge = true;
						}
					}
					if (! judge)
					{
						list.add(chunck);
					}
				}
				else
				{
					System.out.println("File broken:"+ str + "pls check and repair");
				}
			}
		}
	}
	public synchronized void ls(Socket s)
	{
		ArrayList<String> file_list = new ArrayList<String> ();
		for (String str : filemap.keySet())
		{
			file_list.add(str);
		}
		try {
			ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
			output.writeObject(file_list);
			output.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println("Client Connection lost");
		}
		
	}
	public synchronized void addDataNode(DataNodeInfo info)
	{
		if (!StorageMap.containsKey(info))
		{
			//System.out.println(info.hostname);
			dataNodeMap.add(info);
			StorageMap.put(info, 0);
		}
		
		
	}
	
	private int getChunckId(DFSfile file)
	{
		
		int id = 0;
		for (int i = 0; i< file.chuncklist.size();i++)
		{
			if (file.chuncklist.get(i).size() < nodeInfo.factor)
			{
				return id;
			}
			else id ++;
				
		}
		return id;
	}
	public synchronized Chunck  getNextChunck()
	{	
		int min = Integer.MAX_VALUE;
		DataNodeInfo tmp = null;
		for (DataNodeInfo d : StorageMap.keySet())
		{
			if (StorageMap.get(d) < min)
			{
				tmp = d;
				min = StorageMap.get(d);
			}
		}
		//System.out.println(tmp.read_port);
		Chunck next_chunck = new Chunck (tmp);
		int value = StorageMap.get(tmp);
		StorageMap.put(tmp, ++value);
		return next_chunck;
		
		
	}
	public ArrayList<DataNodeInfo> uploadFile(Chunck f)
	{
		ArrayList<DataNodeInfo> list = new ArrayList<DataNodeInfo> ();
		//load balancer
		//find the minimum three
		//how ? define
		HashMap <Integer, DataNodeInfo> nodemap = new HashMap <Integer, DataNodeInfo> ();
		
		for (int i = 0; i< nodeInfo.factor ; i++)
		{
			find_min (nodemap);
		}
		for (Integer i : nodemap.keySet())
		{
			DataNodeInfo value = nodemap.get(i);
			list.add(value);
		}

		return list;
	}
	public void find_min ( HashMap <Integer, DataNodeInfo> nodemap  )
	{
		int min = Integer.MAX_VALUE;
		DataNodeInfo tmp = null;
		for (DataNodeInfo d : StorageMap.keySet())
		{
			int value = StorageMap.get(d);
			if (value < min && !nodemap.containsKey(value))
			{
				min = value;
				tmp = d;
			}
		}
		nodemap.put(min, tmp);
	}
	
	/*
	 * checkpoint class
	 * */
	private class checkpoint extends Thread
	{
		public checkpoint()
		{
			
		}
		@Override 
		public void run()
		{
			while(true)
			{
				try {

					FileOutputStream out = new FileOutputStream("obj/filemap.obj");
					ObjectOutputStream outObj = new ObjectOutputStream(out);
					outObj.writeObject(filemap);	
					outObj.flush();
					outObj.close();
					
					out = new FileOutputStream("obj/dataNodeMap.obj");
					outObj = new ObjectOutputStream(out);
					outObj.writeObject(dataNodeMap);	
					outObj.flush();
					outObj.close(); 
					

					out = new FileOutputStream("obj/StorageMap.obj");
					outObj = new ObjectOutputStream(out);
					outObj.writeObject(StorageMap);	
					outObj.flush();
					outObj.close(); 
					//System.out.println("finish checkpoint");
					Thread.sleep(1000);
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
	//update stroage map
	private class heartBeat extends Thread
	{
		public heartBeat()
		{
			socket_cache = new HashMap<String, Socket>();
		}
		private HashMap<String, Socket> socket_cache;
		@Override
		public void run()
		{
			while(true){
			synchronized (dataNodeMap)
			{
				ArrayList<DataNodeInfo> list = new ArrayList<DataNodeInfo>();
				//System.out.println("Begin heart beat");
				//System.out.println(dataNodeMap.size());
				for (DataNodeInfo d : dataNodeMap)
				{
					try {
					
						Socket socket = new Socket(d.hostname, d.name_port );
						//AckMessage msg = new AckMessage (true, "Alive");
						PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
						out.write("Alive\n");
						out.flush();
						ObjectInputStream in = new ObjectInputStream ( socket.getInputStream() );
						HashMap<String, Chunck> map = (HashMap<String, Chunck>)in.readObject();
						//update storage map;
						StorageMap.put(d, map.size());						
						//Thread.sleep(1000);
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						/*Some node breaks
						 * 
						 * */
						list.add(d);
						continue;
						// TODO Auto-generated catch block
						//e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					
				}
				//System.out.println(list.size());
				
				for(DataNodeInfo d: list)
				{	
					//System.out.println(d.hostname);
					//System.out.println(d.download_port);
					dataNodeMap.remove(d);
				}
				
				
				
				try {
					synchronized (filemap){
						updateFile(list);
					}
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			}
		}
	}
	
	private void updateFile (ArrayList<DataNodeInfo> list) throws UnknownHostException, IOException
	{
		//HashMap<DFSfile, ArrayList<Integer>> file_list = HashMap<DFSfile, Integer> ;
		//System.out.println("Before update");
		/*for(String str :filemap.keySet())
		{
			DFSfile file = filemap.get(str);
			int count = 0;
			for(ArrayList<Chunck> tmp : file.chuncklist )
			{
				System.out.println("Chunck ID:" + count);
				for (Chunck c : tmp)
				{
					System.out.print(c.nodeInfo.download_port + " ");
				}
				count ++ ;
				System.out.println("");
			}
		}*/
		
		for (String str : filemap.keySet())
		{
			for (DataNodeInfo d :list)
			{
				if ( filemap.get(str).nodemap.containsKey(d)){
				ArrayList<Integer> chunck_list = filemap.get(str).nodemap.get(d);
				for (int i : chunck_list)
				{
					for (int j = 0; j < filemap.get(str).chuncklist.get(i).size() ;j ++)
					{
						Chunck tmp = filemap.get(str).chuncklist.get(i).get(j);
						if ( (tmp.nodeInfo.hostname == d.hostname)  && (tmp.nodeInfo.download_port == d.download_port) )
						{
						//	System.out.println("remove");
							filemap.get(str).chuncklist.get(i).remove(j);
						}
					}
					
					
				}
				}
			}	
		}
		
		//System.out.println("After update");
		/*for(String str :filemap.keySet())
		{
			DFSfile file = filemap.get(str);
			int count = 0;
			for(ArrayList<Chunck> tmp : file.chuncklist )
			{
				System.out.println("Chunck ID:" + count);
				for (Chunck c : tmp)
				{
					System.out.print(c.nodeInfo.download_port + " ");
				}
				count ++ ;
				System.out.println("");
			}
		}*/
		
		for (String str : filemap.keySet())
		{
			for (DataNodeInfo d :list)
			{
				ArrayList<Integer> chunck_list = filemap.get(str).nodemap.get(d);
				//System.out.println("Nodemap size:");System.out.println(chunck_list.size());
				
				for (int i : chunck_list)
				{
					//System.out.println("id: " + i);
					//System.out.println("size: " + filemap.get(str).chuncklist.get(i).size());
					
					if ( filemap.get(str).chuncklist.get(i).size() > 0 && filemap.get(str).chuncklist.get(i).size()< nodeInfo.factor ){
						Chunck tmp = filemap.get(str).chuncklist.get(i).get(0);
						DataNodeInfo node = tmp.nodeInfo;
						int iteration = nodeInfo.factor - filemap.get(str).chuncklist.get(i).size();
						//System.out.println(iteration);
						ArrayList<DataNodeInfo> des = new ArrayList<DataNodeInfo>();
						int start = 0;
						while(des.size() < iteration && start < dataNodeMap.size())
						{
							if (dataNodeMap.get(start) != node)
							{
								des.add(dataNodeMap.get(start));
							}
							start ++ ;
						}
						//System.out.println(node.upload_port);
						//System.out.println(node.hostname);
						String name = str + "_chunck" + i;
						DuplicateMessage msg = new DuplicateMessage (name, des);
						Socket s = new Socket (node.hostname, node.upload_port);
						ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
						output.writeObject(msg);
						output.flush();
						DFSfile file = filemap.get(str);
						for (DataNodeInfo data : des)
						{
							Chunck chunck = new Chunck (tmp);
							chunck.nodeInfo = data;
							file.chuncklist.get(i).add(chunck);
						}
						
						
						
						
					}
					
				}
			}	
		}
		
		
		
	}
	

}
