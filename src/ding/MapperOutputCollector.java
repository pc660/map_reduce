package ding;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;



public class MapperOutputCollector<K, V> implements OutputCollector<K, V> {
	private int numOfReducer;
	private String filePrefix;
	private PrintWriter[] printers; 
	private ArrayList<ArrayList<KVPair>> tmps;
	
	public MapperOutputCollector (String filePrefix, int numOfReducer) {
		this.numOfReducer = numOfReducer;
		this.filePrefix = filePrefix;
		printers = new PrintWriter[numOfReducer];
		for (int i = 0; i < printers.length; i++) {
			String pathname = filePrefix + "_" + i;
			try {
				printers[i] = new PrintWriter(new File(pathname));
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
	}

}
