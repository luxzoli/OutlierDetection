package grid;

import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import grid.Entry;
import grid.EntryComparator;

public class GridCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	private static Random random;
	private static PriorityQueue<Entry> queue;
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		int sampleSize = Integer.parseInt(conf.get("sampleSize"));
		//int numSamples = Integer.parseInt(conf.get("numSamples"));
		random = new Random(new Date().getTime());
		 Comparator<Entry> comparator = new EntryComparator();
			queue = new PriorityQueue<Entry>((sampleSize),comparator);
		for (Text value : values) {
			double priority = random.nextDouble();
			if(queue.size() >= (sampleSize)){
				queue.poll();
			}
			Entry entry = new Entry(new Text(value.toString()),priority);
			queue.add(entry);
		}
		int size = queue.size();
		for(int i = 0; i < (size);i++){
			Entry entry = queue.poll();
			//System.out.println((key));
			context.write(key, entry.getValue());
		}
		
	}
}
