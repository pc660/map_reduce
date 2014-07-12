package ding;

public class KVPair implements Comparable<KVPair> {
	private String key;
	private String value;
	public KVPair(String line) {
		line = line.trim();
		String[] parts = line.split("\t");
		this.key = parts[0];
		this.value = parts[1];	
	}
	public KVPair(String key, String value) {
		this.key = key.trim();
		this.value = value.trim();
	}
	
	@Override
	public int compareTo(KVPair o) {
		// TODO Auto-generated method stub
		return this.key.compareTo(o.getKey());
	}
	public String getKey() {
		return key;
	}
	public String getValue() {
		return value;
	}
	public String toString() {
		return key + "\t" + value;
	}
	
}
