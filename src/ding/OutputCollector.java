package ding;
import java.io.IOException;

/**
 * 
 * @author dingma
 *
 * @param <K>	Output key type
 * @param <V>	Output value type
 */

public interface OutputCollector<K, V> {
	
	
	public void collect(K key, V value) throws IOException;
	public void close();
}
