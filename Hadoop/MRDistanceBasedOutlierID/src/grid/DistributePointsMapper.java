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

public class DistributePointsMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	// private static Pattern inputPattern = Pattern.compile("(.*) (\\d*)");
	private Grid grid = null;
	private float epsilon;
	private Text outValue = new Text();

	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		this.epsilon = Float.parseFloat(conf.get("epsilon"));
		String gridAsString = conf.get("gridAsString");
		/*
		 * Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		 * FileInputStream fileStream = new FileInputStream(
		 * cacheFiles[0].toString()); BufferedReader br = new BufferedReader(
		 * new InputStreamReader(fileStream));
		 */
		// The grid is written in a single line
		// String gridAsString = br.readLine();
		// gridAsString = gridAsString.substring(gridAsString.indexOf("\t") +
		// 1);
		// br.close();
		this.grid = Grid.readFromString(gridAsString);

	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {		
		String valueAsString = value.toString();
		String pointAsString = valueAsString.substring(valueAsString
				.indexOf("\t") + 1);
		Point point = new Point(pointAsString);
		ArrayList<Grid> matchingGrids = grid.getMatchingGrids(point, epsilon);
		boolean notOnceCore = true;
		
		IntWritable outKey = new IntWritable();
		for (Grid g : matchingGrids) {
			if (g.isInside(point) && notOnceCore) {
				outValue.set(pointAsString + " core");
				outKey.set(g.getID());
				context.write(outKey, outValue);
				notOnceCore = false;
			} else {
				outValue.set(pointAsString + " border");
				outKey.set(g.getID());
				context.write(outKey, outValue);
			}

		}
	}
}
