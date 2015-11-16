package outlier;

import java.io.IOException;
import java.util.ArrayList;

import kdtree.KDTree;
import kdtree.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistanceBasedOutliersReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		float epsilon = Float.parseFloat(conf.get("epsilon"));
		int pi = Integer.parseInt(conf.get("pi"));
		ArrayList<Point> pointsAL = new ArrayList<Point>();
		for (Text value : values) {
			Point p = new Point(new String(value.toString()), " ");
			pointsAL.add(p);
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
		// ArrayList<Point> neighbors = new ArrayList<Point>();
		for (Point p : points) {
			if (p.isCorePoint()) {
				
				if(!KDTree.epsilonNeighborhoodCheck(tree, p, pi, epsilon)){
					context.write(key, new Text(p.toString()));
				}
				/*float kDistance = KDTree.kDistance(tree, p, pi);
				if (kDistance > epsilon) {
					context.write(key, new Text(p.toString()));
				}*/
			}

			// KDTree.epsilonNeighborhood(tree, p, epsilon, neighbors);
			// if((neighbors.size() < pi) && p.isCorePoint()){
			// context.write(key, new Text(p.toString()));
			// }
			// neighbors.clear();
			// System.out.println(neighbors.size());
		}
	}
}