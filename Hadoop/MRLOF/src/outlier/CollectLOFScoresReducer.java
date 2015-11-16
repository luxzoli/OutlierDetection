package outlier;

import java.io.IOException;

import kdtree.Point;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CollectLOFScoresReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			Point p = Point.fromLOFString(value.toString(), " ");
			context.write(key, new Text(p.toSimpleString() + " " + p.getLOFScore()));
		}

	}
}