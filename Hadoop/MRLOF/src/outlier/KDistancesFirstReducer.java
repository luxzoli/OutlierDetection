package outlier;

import java.io.IOException;
import java.util.ArrayList;

import kdtree.KDTree;
import kdtree.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KDistancesFirstReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int pi = Integer.parseInt(conf.get("pi"));
		int cellID = key.get();
		long pointID = 0;
		ArrayList<Point> pointsAL = new ArrayList<Point>();
		for (Text value : values) {
			Point p = new Point(new String(value.toString()), " ",true);
			p.setID(pointID);
			p.setCellID(cellID);
			pointsAL.add(p);
			pointID++;
		}
		Point[] points = new Point[pointsAL.size()];
		int i = 0;
		for (Point p : pointsAL) {
			points[i] = p;
			i++;
		}
		pointsAL.clear();
		pointsAL = null;
		KDTree tree = new KDTree(points, 10);
		ArrayList<Point> neighbors = new ArrayList<Point>();
		for (Point p : points) {
			float kDistance = KDTree.exclusiveKNNQuery(tree, p, pi, neighbors);
			p.setkDistance(kDistance);
			Point[] knn = new Point[neighbors.size()];
			for(int j = 0;j < neighbors.size();j++){
				knn[j] = neighbors.get(j);
			}
			p.setNearestNeighbors(knn);
			if (KDTree.iskDistanceReady(tree, p, kDistance)) {
				p.setReady(true);
			} else {
				p.setReady(false);
			}
			context.write(key, new Text(p.toString() + "#" + p.getID() + " " + cellID));
			neighbors.clear();
		}
	}
}