package outlier;

import java.io.IOException;

import kdtree.Point;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CollectLOFScoresReducer extends
		Reducer<IntWritable, Text, LongWritable, Text> {
	
	private LongWritable outKey = new LongWritable();
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			Point p = Point.fromLOFString(value.toString(), " ");
			outKey.set(p.getID());
			context.write(outKey, new Text(p.toCoordsString() + " " + p.getLOFScore()));
		}

	}
}