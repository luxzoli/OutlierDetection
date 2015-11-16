package outlier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import kdtree.KDTree;
import kdtree.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LOFFirstReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int pi = Integer.parseInt(conf.get("pi"));
		ArrayList<Point> pointsAL = new ArrayList<Point>();
		for (Text value : values) {
			String pointAsString = value.toString();
			StringTokenizer st = new StringTokenizer(pointAsString,"#");
			pointAsString = st.nextToken();
			String IDString = st.nextToken();
			StringTokenizer idTokenizer = new StringTokenizer(IDString," ");
			Long pointID = Long.parseLong(idTokenizer.nextToken());
			int cellID = key.get();
			Point p = Point.fromLRDString(pointAsString, " ");
			p.setCellID(cellID);
			p.setID(pointID);
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
		ArrayList<Point> neighbors = new ArrayList<Point>();
		for (Point p : points) {
			float kDistance = KDTree.exclusiveKNNQuery(tree, p, pi, neighbors);
			Point[] knn = new Point[neighbors.size()];
			for(int j = 0;j < neighbors.size();j++){
				knn[j] = neighbors.get(j);
			}
			p.setNearestNeighbors(knn);
			p.calculateLOFScore();
			if (KDTree.iskDistanceReady(tree, p, kDistance)) {
				p.setReady(true);
			} else {
				p.setReady(false);
			}
			context.write(key, new Text(p.toLOFString() + "#" + p.getID() + " " + key.get()));
			neighbors.clear();
		}
	}
}