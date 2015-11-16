package outlier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import kdtree.Grid;
import kdtree.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LOFSecondMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private IntWritable outKey = new IntWritable();
	ArrayList<Grid> matching;;
	private Grid grid = null;
	private Text outValue = new Text();

	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		String gridAsString = conf.get("gridAsString");
		matching = new ArrayList<Grid>();
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
		int originalID = Integer.parseInt(valueAsString.substring(0,
				valueAsString.indexOf("\t")));
		String pointAsString = valueAsString.substring(valueAsString
				.indexOf("\t") + 1);
		StringTokenizer st = new StringTokenizer(pointAsString, "#");
		pointAsString = st.nextToken();
		String IDString = st.nextToken();
		Point p = Point.fromLOFString(pointAsString, " ");
		
		float kDistance = p.getkDistance();
		// nem kör
		if (p.isReady()) {
			outKey.set(originalID);
			outValue.set(pointAsString + "o" + "#" + IDString);
			context.write(outKey, outValue);
		} else {
			Grid.getMatchingGrids(grid, p, kDistance, matching);
			for (Grid match : matching) {
				int ID = match.getID();
				outKey.set(ID);
				if (ID == originalID) {
					outValue.set(pointAsString + "o#" + IDString);
				} else {
					outValue.set(pointAsString + "g#" + IDString);
				}
				context.write(outKey, outValue);
			}
			matching.clear();
		}
		
	}
}
