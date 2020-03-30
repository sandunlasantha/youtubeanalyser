package YouTubeAnalyser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends 	Reducer<Text,Text,Text,IntWritable>{
	
	public void reduce(Text key, Iterable<Text> val, Context con) throws IOException,InterruptedException {
		int count = 0;
		 for (Text vCount : val) {
			 count = count+1;
		 }
		
		 con.write(key, new IntWritable(count));
	}

	
}
