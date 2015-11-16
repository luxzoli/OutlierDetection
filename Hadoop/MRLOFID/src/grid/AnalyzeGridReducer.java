package grid;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalyzeGridReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<IntWritable> iterator = values.iterator();
		int sum = 0;
		while (iterator.hasNext()) {
			IntWritable value = iterator.next();
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
	
	

}