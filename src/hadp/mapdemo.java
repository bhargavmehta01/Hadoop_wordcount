package hadp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class mapdemo extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws 
    	IOException, InterruptedException {
    	
      String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
    	  String str = itr.nextToken().toString().toLowerCase();
    	  if(str.contains("education")){
    	        word.set(fname+":education");
    	        context.write(word, one);   
    	  }
    	  if(str.contains("politics")){
	  	        word.set(fname+":politics");
	  	        context.write(word, one);   
    	  }
    	  if(str.contains("sports")){
    	        word.set(fname+":sports");
    	        context.write(word, one);   
      	  }
    	  if(str.contains("agriculture")){
    	        word.set(fname+":agriculture");
    	        context.write(word, one);   
      	  }
      }
    }
}