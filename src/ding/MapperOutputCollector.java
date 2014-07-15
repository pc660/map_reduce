package ding;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import File_system.DistributedFileSystem;

/**
 * 
 * @author dingma
 *
 * @param <K>
 * @param <V>
 */

public class MapperOutputCollector<K, V> implements OutputCollector<K, V> {
	
	private int numOfReducer;
	private String filePrefix;
	private PrintWriter[] printers; 
	private ArrayList<ArrayList<KVPair>> tmps;
	private ArrayList<String> pathnames;
	
	/**
	 * 
	 * @param filePrefix
	 * @param numOfReducer
	 */
	
	public MapperOutputCollector (String filePrefix, int numOfReducer) {
		this.numOfReducer = numOfReducer;
		this.filePrefix = filePrefix;
		printers = new PrintWriter[numOfReducer];
		pathnames = new ArrayList<String>();
		for (int i = 0; i < printers.length; i++) {
			String pathname = filePrefix + "_" + i;
			try {
				File file = new File(pathname);
				printers[i] = new PrintWriter(file);
				pathnames.add(file.getPath());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("Cannot create the file output stream");
			}
		}
		tmps = new ArrayList<ArrayList<KVPair>>();
		for (int i = 0; i < printers.length; i++) {
			tmps.add(new ArrayList<KVPair>());	
		}
	}
	@Override
	
	
	public void collect(K key, V value) throws IOException {
		// TODO Auto-generated method stub
		int hashcode = Math.abs(key.toString().hashCode());
		int index = hashcode % this.tmps.size();
		tmps.get(index).add(new KVPair(key.toString(), value.toString()));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		for (int i = 0; i < this.tmps.size(); i++) {
			ArrayList<KVPair> list = tmps.get(i);
			Collections.sort(list);
			for (KVPair pair : list) {
				printers[i].println(pair.toString());
			}
			printers[i].close();
		}
		printers= null;
		tmps = null;
		
		uploadToDFS(pathnames);
	}
	public void uploadToDFS(ArrayList<String> paths){
		DistributedFileSystem dfs = new DistributedFileSystem();
		for (String path : paths) {
			dfs.uploadFile(path);
		}
	}

}
