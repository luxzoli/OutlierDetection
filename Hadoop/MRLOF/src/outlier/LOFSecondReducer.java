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

public class LOFSecondReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int pi = Integer.parseInt(conf.get("pi"));
		ArrayList<Point> pointsOriginalAL = new ArrayList<Point>();
		ArrayList<Point> pointsGuestAL = new ArrayList<Point>();
		for (Text value : values) {
			String pointAsString = value.toString();
			StringTokenizer st = new StringTokenizer(pointAsString,"#");
			pointAsString = st.nextToken();
			String IDString = st.nextToken();
			StringTokenizer idTokenizer = new StringTokenizer(IDString," ");
			Long pointID = Long.parseLong(idTokenizer.nextToken());
			int cellID =Integer.parseInt(idTokenizer.nextToken());
			if(pointAsString.endsWith("o")){
				pointAsString = pointAsString.substring(0, pointAsString.length() -1);
				Point p = Point.fromLOFString(pointAsString, " ");
				pointsOriginalAL.add(p);
			}else {
				pointAsString = pointAsString.substring(0, pointAsString.length() -1);
				Point p = Point.fromLOFString(pointAsString, " ");
				p.setCellID(cellID);
				p.setID(pointID );
				pointsGuestAL.add(p);
			}
			
		}
		Point[] pointsO = new Point[pointsOriginalAL.size()];
		Point[] pointsG = new Point[pointsGuestAL.size()];
		int i = 0;
		for (Point p : pointsOriginalAL) {
			pointsO[i] = p;
			i++;
		}
		i = 0;
		for (Point p : pointsGuestAL) {
			pointsG[i] = p;
			i++;
		}
		pointsOriginalAL.clear();
		pointsOriginalAL = null;
		pointsGuestAL.clear();
		pointsGuestAL = null;
		KDTree tree = new KDTree(pointsO, 10);
		ArrayList<Point> neighbors = new ArrayList<Point>();
		//TODO: calculate lof
		for (Point p : pointsG) {
			float LOF = 0;
			//float LRD = p.getLocalReachabilityDensity();
			KDTree.kNNQuery(tree, p, pi, neighbors);
			p.setReady(false);
			Point[] knn = new Point[neighbors.size()];
			for(int j = 0;j < neighbors.size();j++){
				Point o = neighbors.get(j);
				knn[j] = o;
			}
			p.setNearestNeighbors(knn);
			for(int j = 0;j < neighbors.size();j++){
				Point o = neighbors.get(j);
				knn[j] = o;
				//LOF += (o.getLocalReachabilityDensity() / LRD / ((float)neighbors.size()));
			}
			p.setLOFScore(LOF);
			context.write(key, new Text(p.toLOFString() + "#" + p.getID() + " " + p.getCellID()));
			neighbors.clear();
		}
	}
}