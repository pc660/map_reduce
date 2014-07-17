package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.io.IOException;

import ding.*;
public class wordcount {


	public static class Map extends Mapper<String, String, String, String>
	{
		public void map(String key, String value, OutputCollector<String, String> out) throws IOException
		{
			String line = value;
			String[] list =  line.split("\\s+");
			if (list != null && list.length > 0) {
				for (String word : list) {
					if (word.matches("^[a-zA-Z0-9]*$")) {
						out.collect(word, 1+"");
					}
				}
			}

			//System.out.println("test for map");

		}
	}
	public static class Reduce extends Reducer<String, String, String, String>
	{
		@Override
		public void reduce(String key, Iterator<String> values, OutputCollector<String, String> out) throws Exception
		{
			int count = 0;
			
			while( values.hasNext()) {
				int i = Integer.parseInt((String) values.next());
				
				count = count + i;
			}
			
			String value = count+"";
			//System.out.println("key" + key + " value" + value);
			out.collect(key, value);

		}

	}
}
