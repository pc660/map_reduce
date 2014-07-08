package File_system;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import Communication.Comm;
import NameNode.NameNodeInfo;
import file.Chunck;
import file.DFSfile;

public class DistributedFileSystem {
	
	Comm communication;
	public String configure_file = "configure.xml";
	NameNodeInfo nodeinfo;
	
	
	public DistributedFileSystem()
	{
		File fXmlFile = new File(configure_file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("NameNode");
			Node node = nList.item(0);
			Element eElement = (Element) node;
			nodeinfo = new NameNodeInfo();
			nodeinfo.hostname =  eElement.getElementsByTagName("hostname").item(0).getTextContent();
			nodeinfo.port = Integer.parseInt(eElement.getElementsByTagName("port").item(0).getTextContent());
			nodeinfo.factor = Integer.parseInt(eElement.getElementsByTagName("factor").item(0).getTextContent());
			nodeinfo.chunck_size = Integer.parseInt(eElement.getElementsByTagName("max_size").item(0).getTextContent());
			
			communication = new Comm();
			communication.max_size = nodeinfo.chunck_size;
			communication.NameNode_address = eElement.getElementsByTagName("hostname").item(0).getTextContent();
			communication.NameNode_read_port = Integer.parseInt(eElement.getElementsByTagName("port").item(0).getTextContent());
		//	nodeInfo.factor = Integer.parseInt( eElement.getElementsByTagName("replica_factor").item(0).getTextContent());
			
			
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public DFSfile getFile (String filename)
	{
		DFSfile file = null;
		try {
				file = communication.getFile(filename);
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
		return file;
	}
	public void uploadFile (String filename)
	{
		try {
			File file = new File ("data/" + filename);
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = "";
			ArrayList<String> text = new ArrayList<String> ();
			
			//byte [] buffer = new byte [count];
			//input.read(buffer);
			DFSfile dfsfile = communication.getNewFile(filename);
			//System.out.println(dfsfile.filename);
			int startIndex = 0;
			int endIndex = 0;
			
			int max_size = nodeinfo.chunck_size;
			while ((line = br.readLine()) != null) {
				text.add(line);
				if (endIndex - startIndex == nodeinfo.chunck_size )
				{
					startIndex = endIndex + 1;
					for (int i = 0;i< nodeinfo.factor;i++){
						
						Chunck chunck = getNextChunck(dfsfile);
						chunck.startIndex = startIndex;
						chunck.endIndex = chunck.endIndex;
						String chunck_name = dfsfile.filename + "_chunck" + chunck.id;
						chunck.chunckname = chunck_name;
						communication.uploadChunck(text, chunck);
						
					}
					text.clear();
				}
				endIndex ++ ;
			}
			if (endIndex - startIndex< nodeinfo.chunck_size){
			for (int i = 0;i< nodeinfo.factor;i++){
				Chunck chunck = getNextChunck(dfsfile);
				String chunck_name = dfsfile.filename + "_chunck" + chunck.id;
				chunck.chunckname = chunck_name;
				System.out.println(chunck.chunckname);
				communication.uploadChunck(text, chunck);
			}
			}
			
		} catch (FileNotFoundException e) {
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
	public void catFile (String filename)
	{
		try {
			ArrayList<String> list  = communication.catFile(filename);
			for (int i = 0; i< list.size() ; i++)
			{
				System.out.println(list.get(i));
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
	}
	public Chunck getNextChunck(DFSfile file)
	{
		Chunck chunck = communication.getNextChunck(file);
		return chunck;
	}
	public void removeFile (String filename)
	{
		if (communication.removeFile(filename))
		{
			System.out.println("remove success");
		}
		else 
			System.out.println("remove failed");
	}
	public ArrayList<String> ls()
	{
		return communication.ls();
	}
	public void DownloadFile (String src, String des)
	{
		try {
			ArrayList<String> list  = communication.catFile(src);
			File file = new File (des);
			PrintWriter output = new PrintWriter (file);
			for (String str : list)
			{
				output.write(str + "\n");
			}
			output.close();
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
	public void showFileInfo(String filename)
	{
		try {
			DFSfile file = communication.getFile(filename);
			for (int i = 0; i< file.chuncklist.size();i ++)
			{
				for (Chunck tmp : file.chuncklist.get(i))
				{
					System.out.println("Chunck id:" + tmp.id  + "is stored in " + tmp.nodeInfo.rootdirectory);
				}
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
		
	}
	public ArrayList<String> readFile (String filename, int start)
	{
		ArrayList<String> list  = communication.readfile(filename, start, -1);
		return list;
	}
	public ArrayList<String> readFile (String filename, int start, int end)
	{
		ArrayList<String> list = communication.readfile (filename, start , end);
		return list;
	}
	
}
