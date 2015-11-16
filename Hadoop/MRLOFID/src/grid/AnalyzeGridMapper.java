package grid;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import point.Grid;
import point.Point;

public class AnalyzeGridMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueAsString = value.toString();
		String keyAsString = valueAsString.substring(0,valueAsString
				.indexOf("\t"));
		int oKey = Integer.parseInt(keyAsString);
		IntWritable outValue = new IntWritable(1);
		IntWritable outKey = new IntWritable(oKey);
		context.write(outKey, outValue);
		
	}
}
