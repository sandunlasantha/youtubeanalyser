package YouTubeAnalyser;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	public void map(LongWritable key,org.w3c.dom.Text value,Context con) throws IOException, InterruptedException {
		
		String line = value.toString();
		String linepart[] = line.split(",");
		
		String id = linepart[0];
		String cat = linepart[3];
		
		con.write(new Text(cat), new Text(id));
		
		
	}

}
