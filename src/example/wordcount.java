package example;
import ding.*;
public class wordcount {
	

		 public static class map extends Mapper
		 {
			 public void map()
			 {
				 System.out.println("test for map");
			 }
		 }
		 public static class reduce extends Reducer
		 {
			 public void reduce()
			 {
				 System.out.println("test for reduce");
			 }
		 }
		 
		 
			 
	
	

}
