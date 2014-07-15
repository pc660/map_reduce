package ding;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

import utilities.FileTool.KeyValuePair;
import file.Chunck;
import file.DFSfile;
import File_system.ChunckReader;
import File_system.DistributedFileSystem;
import MessageForMap.Message;

public class Tools {
	public  boolean downloadFileToLocal(String mapperHost, int mapperPort, String remoteFilename,
			String localFilename, String localDir) {
		String localFilePath = localDir + localFilename;

		File newFile = null;
		FileOutputStream writeStream = null;
		newFile = new File(localFilePath);
		writeStream = new FileOutputStream(newFile);

		DataTransferMessage outcomingMessage = new DataTransferMessage(remoteFilename, null);

		Socket socket = createSocket(mapperHost, mapperPort);
		DataTransferMessage incomingMessage = null;
		if (sendMessage(socket, outcomingMessage)) {
			incomingMessage = (DataTransferMessage) receiveMessage(socket);
		}
		if (incomingMessage.getPayload() != null) {
			byte[] payload = (byte[]) incomingMessage.getPayload();
			writeStream.write(payload);
		}
		closeFileStream(writeStream);
		return true;
	}

	public boolean downloadAllFileToLocal() {

	}

	public String downloadDFSToLocal(String fileName) {
		DistributedFileSystem dfs = new DistributedFileSystem();
		DFSfile dfsFile =  dfs.getFile(fileName);
		ArrayList<ArrayList<Chunck>> lists= dfsFile.chuncklist;
		File file = null;
		//	FileOutputStream writeStream = null;
		file = new File(fileName);
		//	writeStream = new FileOutputStream(newFile);

		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		for (ArrayList<Chunck> list : lists) {
			ChunckReader chunckReader = new ChunckReader(list.get(0));
			String line = null;
			while ((line = chunckReader.readline()) != null) {
				bw.write(line);
			}
		}
		bw.close();
		return file.getCanonicalPath();
	}

	public String mergeSortedFiles(String pathname1, String pathname2, String mergedFileName) {
		File file1 = null;
		File file2 = null;
		File mergedFile = null;
		Scanner scanner1 = null;
		Scanner scanner2 = null;
//		String tmpFileName = mergedFileName;

		file1 = new File(pathname1);
		file2 = new File(pathname2);
		scanner1 = new Scanner(file1);
		scanner2 = new Scanner(file2);
		mergedFile = new File(mergedFileName);
		PrintWriter mergedFilePrinter = new PrintWriter(mergedFile);

		KVPair kvpair1 = null;
		KVPair kvpair2 = null;
		String line1 = null;
		String line2 = null;	
		if (scanner1.hasNextLine()) {
			line1 = scanner1.nextLine();
			kvpair1 = new KVPair(line1);
		}
		if (scanner2.hasNextLine()) {
			line2 = scanner2.nextLine();
			kvpair2 = new KVPair(line2);
		}

		while (line1 != null  && line2 != null) {
			int comp = kvpair1.compareTo(kvpair2);
			if (comp < 0) {
				mergedFilePrinter.println(kvpair1.toString());
				if (scanner1.hasNext()) {
					line1 = scanner1.nextLine();
					kvpair1 = new KVPair(line1);
				} else {
					line1 = null;
					kvpair1 = null;
				}
			} else {
				mergedFilePrinter.println(kvpair2.toString());
				if (scanner2.hasNext()) {
					line2 = scanner2.nextLine();
					kvpair2 = new KVPair(line2);
				} else {
					line2 = null;
					kvpair2 = null;
				}
			}
		}
		

		
		while (line1 != null) {
			kvpair1 = new KVPair(line1);
			mergedFilePrinter.println(kvpair1.toString());
			line1 = scanner1.nextLine();
		}
		
		while (line2 != null) {
			kvpair2 = new KVPair(line2);
			mergedFilePrinter.println(kvpair2.toString());
			line2 = scanner2.nextLine();
		}
		
		file1.delete();
		file2.delete();
		scanner1.close();
		scanner2.close();
		mergedFilePrinter.close();
		return mergedFile.getCanonicalPath();
		
	}

	public void closeFileStream(FileOutputStream fileStream) {
		if (fileStream != null) {
			fileStream.close();
		}
	}

	public  Socket createSocket(String host, int port) {
		Socket socket = null;

		socket = new Socket(host,port);


		return socket;

	}

	public boolean sendMessage(Socket socket, Message message) {

		ObjectOutputStream oos = null;
		oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(message);
		oos.flush();
		return true;
	}

	public Message receiveMessage(Socket socket) {
		ObjectInputStream ois = null;
		ois = new ObjectInputStream(socket.getInputStream());
		Message incomingMessage = (Message) ois.readObject();
		return incomingMessage;
	}


}
