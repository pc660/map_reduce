package example;
import java.io.IOException;

import ding.*;
public class wordcount {
	

		 public static class Map extends Mapper<String, String, String, String>
		 {
			 @Override	 
			 public void map (String key, String value, OutputCollector<String, String> output) throws IOException
			 {
				// System.out.println(key);
				 output.collect(key, value);
			 }
		 }
		 public static class Reduce extends Reducer
		 {
			 public void reduce()
			 {
				 System.out.println("test for reduce");
			 }
		 }
		 
		 
			 
	
	

}
