package outlier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import kdtree.Point;
import kdtree.PointEntry;
import kdtree.PointEntryComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterLRDReducer extends
		Reducer<Text, Text, IntWritable, Text> {
	
	private ArrayList<Point> pointsAL = new ArrayList<Point>();
	private IntWritable outKey = new IntWritable();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int pi = Integer.parseInt(conf.get("pi"));
		
		for (Text value : values) {
			String pointAsString = value.toString();
				pointAsString = pointAsString.substring(0, pointAsString.length() -1);
				pointAsString = value.toString();
				String IDString = key.toString();
				StringTokenizer idTokenizer = new StringTokenizer(IDString," ");
				Long pointID = Long.parseLong(idTokenizer.nextToken());
				int cellID =Integer.parseInt(idTokenizer.nextToken());
				Point p = Point.fromLRDString(pointAsString, " ");
				p.setCellID(cellID);
				p.setID(pointID);
				pointsAL.add(p);
		}
		int k = pi;
		PriorityQueue<PointEntry> kNearestNeighbors = new PriorityQueue<PointEntry>(k,new PointEntryComparator());
		float kDistance = Float.MAX_VALUE;
		for(Point p : pointsAL){
		for (Point o : p.getNearestNeighbors()) {
			float distance = Point.euclideanDistance(o, p);
			if (kDistance > distance) {
				PointEntry pe = new PointEntry(o, distance);
				kNearestNeighbors.offer(pe);
				if (kNearestNeighbors.size() > k) {
					kDistance = kNearestNeighbors.peek().getDistance();
					PointEntry[] tempRemoved = new PointEntry[kNearestNeighbors
							.size()];
					int index = 0;
					while (Math.abs(kNearestNeighbors.peek().getDistance() - kDistance) < 0.000001) {
						tempRemoved[index] = kNearestNeighbors.poll();
						index++;
					}
					if (kNearestNeighbors.size() < k) {
						for (int j = 0; j < index; j++) {
							kNearestNeighbors.add(tempRemoved[j]);
						}
					} else {
						kDistance = kNearestNeighbors.peek().getDistance();
					}
				} else if(kNearestNeighbors.size() == k){
					kDistance = kNearestNeighbors.peek().getDistance();
				}
			}
			else if (Math.abs(kDistance - distance) < 0.000001) {
				PointEntry pe = new PointEntry(o, distance);
				kNearestNeighbors.offer(pe);
			}
		}
		}
		kDistance = kNearestNeighbors.peek().getDistance();
		Point[] knn = new Point[kNearestNeighbors.size()];
		int size = kNearestNeighbors.size();
		for(int j = 0;  j < size; j++){
			knn[j] = kNearestNeighbors.poll().getPoint();
		}
		Point p = pointsAL.get(0);
		p.setNearestNeighbors(knn);
		p.calculateLRD();
		pointsAL.clear();
		outKey.set(p.getCellID());
		p.setReady(true);
		context.write(outKey, new Text(p.toLRDString() + "#" + p.getID() + " " + p.getCellID()));
	}
}