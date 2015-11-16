package grid;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BoundariesMapper extends Mapper<LongWritable, Text, IntWritable,Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String valueAsString = value.toString();
		String pointAsString = valueAsString.substring(valueAsString
				.indexOf("\t") + 1);
		Text outValue = new Text(pointAsString);
	  context.write(new IntWritable(0), outValue);

  }
}
