package mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import MessageForMap.JobMessage;

public class JobClient {
	public String hostname; 
	public int jobtrackerport;
	
	
	
	public void runJob(Jobconfig config) throws IOException
	{
		/*
		 * Send jobconfig to the master node
		 * */
		
		/*
		 * hard code here
		 * */
		
		File fXmlFile = new File("mapreduce.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		//System.out.println("123");
		
			try {
				dBuilder = dbFactory.newDocumentBuilder();
				org.w3c.dom.Document doc;
				doc = dBuilder.parse(fXmlFile);
				doc.getDocumentElement().normalize();
				NodeList nList = doc.getElementsByTagName("master");
				//System.out.println(nList.getLength());
				
				Node node = nList.item(0);
				Element eElement = (Element) node;
				//nodeInfo = new NameNodeInfo();
			//	hostname = eElement.getElementsByTagName("masterhost").item(0).getTextContent();
				this.jobtrackerport = Integer.parseInt(eElement.getElementsByTagName("receiver_port").item(0).getTextContent());
				this.hostname = eElement.getElementsByTagName("masterhost").item(0).getTextContent();
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JobMessage msg = new JobMessage();
			msg.config = config;
			System.out.println(config.filename);
			Socket s = new Socket (hostname, jobtrackerport);
			
		ObjectOutputStream output = new ObjectOutputStream (s.getOutputStream());
		output.writeObject(msg);
		
		
	}
}
