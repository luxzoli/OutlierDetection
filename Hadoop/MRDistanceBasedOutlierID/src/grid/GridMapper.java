package grid;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GridMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	// private static Pattern inputPattern = Pattern.compile("(.*) (\\d*)");
	private Random rnd;
	private double probability;
	private long numPoints;
	private IntWritable oKey = new IntWritable(0);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		this.rnd = new Random();
		this.numPoints = Long.parseLong(conf.get("numPoints"));
		int sampleSize = Integer.parseInt(conf.get("sampleSize"));
		int maxLeafSize = Integer.parseInt(conf.get("maxLeafSize"));
		this.probability = ((double) maxLeafSize) / ((double) sampleSize);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueAsString = value.toString();
		String pointAsString = valueAsString.substring(valueAsString
				.indexOf("\t") + 1);
		Text outValue = new Text(pointAsString);
		if (numPoints > 10000000) {
			double randomValue = rnd.nextFloat();
			if (randomValue < (probability * 2.0)) {
				context.write(oKey, new Text(outValue));
			}
		} else {
			context.write(oKey, new Text(outValue));
		}

	}
}
