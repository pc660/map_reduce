package ding;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

import file.Chunck;
import file.DFSfile;
import File_system.ChunckReader;
import File_system.DistributedFileSystem;
import MessageForMap.Message;

public class Tools {
	/*
	public  static boolean downloadFileToLocal(String mapperHost, int mapperPort, String remoteFilename,
			String localFilename, String localDir) {
		String localFilePath = localDir + localFilename;

		File newFile = null;
		FileOutputStream writeStream = null;
		newFile = new File(localFilePath);
		try {
			writeStream = new FileOutputStream(newFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DataTransferMessage outcomingMessage = new DataTransferMessage(remoteFilename, null);

		Socket socket = createSocket(mapperHost, mapperPort);
		DataTransferMessage incomingMessage = null;
		if (sendMessage(socket, outcomingMessage)) {
			incomingMessage = (DataTransferMessage) receiveMessage(socket);
		}
		if (incomingMessage.getPayload() != null) {
			byte[] payload = (byte[]) incomingMessage.getPayload();
			try {
				writeStream.write(payload);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		closeFileStream(writeStream);
		return true;
	}
*/
	public static String downloadDFSToLocal(String fileName, ArrayList<String> log) {
		DistributedFileSystem dfs = new DistributedFileSystem();
		DFSfile dfsFile =  dfs.getFile(fileName);
		ArrayList<ArrayList<Chunck>> lists= dfsFile.chuncklist;
		File file = null;
		//	FileOutputStream writeStream = null;
		file = new File(fileName);
		//	writeStream = new FileOutputStream(newFile);
		//System.out.println(fileName);
		// if file doesnt exists, then create it
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.add("Failed during createLocalFile in Tool.downloadDFSToLocal");
				e.printStackTrace();
				return null;
			}
		}
		String retrunPath = null;
		FileWriter fw;
		BufferedWriter bw;
		try {
			fw = new FileWriter(file.getAbsoluteFile());
			//System.out.println("download from DFS " + file.getAbsolutePath());
			bw = new BufferedWriter(fw);
			for (ArrayList<Chunck> list : lists) {
				ChunckReader chunckReader = new ChunckReader(list.get(0));
				String line = null;
				while ((line = chunckReader.readline()) != null) {
					//System.out.println(line);
					bw.write(line + "\n");
				}
			}
			bw.close();
			retrunPath = file.getCanonicalPath();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.add("Failed during readChunck in Tool.downloadDFSToLocal");
			e.printStackTrace();
			return null;
		}
		return retrunPath;
	}

	public static String mergeSortedFiles(String pathname1, String pathname2, String mergedFileName, ArrayList<String> log) {
		File file1 = null;
		File file2 = null;
		File mergedFile = null;
		Scanner scanner1 = null;
		Scanner scanner2 = null;
		
		//		String tmpFileName = mergedFileName;
		
		file1 = new File(pathname1);
		file2 = new File(pathname2);
		try {
			//System.out.println("merge file1 " + file1);
			scanner1 = new Scanner(file1);
			scanner2 = new Scanner(file2);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			log.add("Failed during new scanner1/2 in Tool.mergeSortedFiles");
			e.printStackTrace();
			return null;
		}
		mergedFile = new File(mergedFileName);
		PrintWriter mergedFilePrinter = null;
		try {
			mergedFilePrinter = new PrintWriter(mergedFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			log.add("Failed during new PrintWriter(mergedFile) in Tool.mergeSortedFiles");
			e.printStackTrace();
			return null;
		}

		KVPair kvpair1 = null;
		KVPair kvpair2 = null;
		String line1 = null;
		String line2 = null;	
		if (scanner1.hasNextLine()) {
			//System.out.println(line1);
			line1 = scanner1.nextLine();
			
			line1 = line1.trim();
			String[] parts = line1.split("\t");
			if (parts.length ==2 ){
				kvpair1 = new KVPair(parts[0],parts[1]);
			}
			
		}
		if (scanner2.hasNextLine()) {
			line2 = scanner2.nextLine();
			//line2 = scanner1.nextLine();
			
			line2 = line2.trim();
			String[] parts = line2.split("\t");
			if (parts.length ==2 ){
				kvpair2 = new KVPair(parts[0],parts[1]);
			}
		//	kvpair2 = new KVPair(line2);
		}

		while (line1 != null  && line2 != null) {
			//System.out.println(line1);
			//System.out.println(line2);
			if (kvpair1 == null )
			{
				if (scanner1.hasNextLine()){
					line1 = scanner1.nextLine();
					line1 = line1.trim();
					String[] parts = line1.split("\t");
					if (parts.length ==2 ){
						kvpair1 = new KVPair(parts[0],parts[1]);
					}
				}
				continue;
			}
			else if (kvpair2 == null)
			{
				if (scanner2.hasNext()) {
					line2 = scanner2.nextLine();
					line2 = line2.trim();
					String[] parts = line2.split("\t");
					if (parts.length ==2 ){
						kvpair2 = new KVPair(parts[0],parts[1]);
					}
				} 
				continue;
			}
			
			int comp = kvpair1.compareTo(kvpair2);
			if (comp < 0) {
				mergedFilePrinter.println(kvpair1.toString());
				if (scanner1.hasNext()) {
					line1 = scanner1.nextLine();
					
					line1 = line1.trim();
					String[] parts = line1.split("\t");
					if (parts.length ==2 ){
						kvpair1 = new KVPair(parts[0],parts[1]);
					}
					else continue;
					
					
					//kvpair1 = new KVPair(line1);
				} else {
					line1 = null;
					kvpair1 = null;
				}
			} else {
				mergedFilePrinter.println(kvpair2.toString());
				if (scanner2.hasNext()) {
					line2 = scanner2.nextLine();
					line2 = line2.trim();
					String[] parts = line2.split("\t");
					if (parts.length ==2 ){
						kvpair2 = new KVPair(parts[0],parts[1]);
					}
					else continue;
					//kvpair2 = new KVPair(line2);
				} else {
					line2 = null;
					kvpair2 = null;
				}
			}
		}



		while (line1 != null) {
			kvpair1 = new KVPair(line1);
			mergedFilePrinter.println(kvpair1.toString());
			
			if (scanner1.hasNextLine())
				line1 = scanner1.nextLine();
			else break;
		}

		while (line2 != null) {
			kvpair2 = new KVPair(line2);
			mergedFilePrinter.println(kvpair2.toString());
			if (scanner2.hasNextLine())
				line2 = scanner2.nextLine();
			else break;
		}
		
		//file1.delete();
		//file2.delete();
		scanner1.close();
		scanner2.close();
		mergedFilePrinter.close();
		String returnPath = null;
		try {
			returnPath = mergedFile.getCanonicalPath();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.add("Failed during returnPath = mergedFile.getCanonicalPath(); in Tool.mergeSortedFiles");
			e.printStackTrace();
			return null;
		}
		
		return returnPath;

	}

	public static void closeFileStream(FileOutputStream fileStream) {
		if (fileStream != null) {
			try {
				fileStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	/*
	public static Socket createSocket(String host, int port) {
		Socket socket = null;

		socket = new Socket(host,port);
		return socket;

	}


	public static boolean sendMessage(Socket socket, Message message) {

		ObjectOutputStream oos = null;
		oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(message);
		oos.flush();
		return true;
	}

	public static Message receiveMessage(Socket socket) {
		ObjectInputStream ois = null;
		ois = new ObjectInputStream(socket.getInputStream());
		Message incomingMessage = (Message) ois.readObject();
		return incomingMessage;
	}
	 */

}
