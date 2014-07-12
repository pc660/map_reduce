package ding;
import java.io.Serializable;

/**
 * 
 * @author dingma
 *
 * @param <INKEY>	Type of input key
 * @param <INVALUE>	Type of input value
 * @param <OUTKEY>	Type of output key
 * @param <OUTVALUE>	Type of output value
 */

public abstract class Reducer <INKEY, INVALUE, OUTKEY, OUTVALUE> implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * @param key		Input key
	 * @param value	Input value
	 * @param out		OutputCollector for use to write  data
	 * @throws Exception
	 */
	public void reduce(INKEY key, INVALUE value, 
			OutputCollector<OUTKEY, OUTVALUE> out) throws Exception{
		throw new Exception("Need to implement reducer by users");
	}
}
