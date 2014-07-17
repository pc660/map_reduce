package example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import ding.Mapper;
import ding.OutputCollector;
import ding.Reducer;

public class CountPunctuation {

	public static class Map extends Mapper <String, String, String, String> {
		public void map(String key, String value, OutputCollector<String, String> out) throws IOException {
			HashSet<Character> hs = new HashSet<Character>();
			hs.add(',');
			hs.add('.');
			hs.add('!');
			hs.add('?');
			hs.add(':');
			hs.add(';');
			hs.add('"');
			hs.add('-');
			char[] array = value.toCharArray();
			for (Character c : array) {
				if (hs.contains(c)) {
					out.collect(c+"", 1+"");
				}
			}
		}

	}

	public static class Reduce extends Reducer<String, String, String, String> {
		public void reduce(String key, Iterator<String> values, OutputCollector<String, String> out) throws IOException {
			int count = 0;	
			while( values.hasNext()) {
				int i = Integer.parseInt((String) values.next());
				count = count + i;
			}

			String value = count+"";
			out.collect(key, value);


		}
	}
}
