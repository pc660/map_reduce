package ding;

public abstract class RecordReader<K, V> {
	
	public abstract void close();
	public abstract boolean nextKeyVlaue() throws Exception;
	public abstract K getCurrentKey();
	public abstract V getCurrentValue();
	public boolean init() {
		// TODO Auto-generated method stub
		return false;
	}
	
}
