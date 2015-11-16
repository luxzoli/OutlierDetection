package lof;

import java.util.ArrayList;

import kdtree.KDTree;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import kdtree.Point;

public class LRDF1 implements
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> {
	private int k;

	public LRDF1(int k) {
		this.k = k;
	}

	@Override
	public Iterable<Tuple2<Integer, Point>> call(
			Tuple2<Integer, Iterable<Point>> arg0) throws Exception {
		ArrayList<Point> pointsAL = new ArrayList<Point>();
		for (Point p : arg0._2) {
			pointsAL.add(p);
		}
		Point[] points = new Point[pointsAL.size()];
		int i = 0;
		for (Point point : pointsAL) {
			// lehet nem kell
			point.setCellID(arg0._1);
			//
			points[i] = point;
			i++;
		}
		pointsAL = null;
		KDTree tree = new KDTree(points, 10);
		ArrayList<Point> neighbors = new ArrayList<Point>();
		ArrayList<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		for (Point point : points) {
			float kDistance = KDTree.exclusiveKNNQuery(tree, point, k,
					neighbors);
			Point[] knn = new Point[neighbors.size()];
			for (int j = 0; j < neighbors.size(); j++) {
				knn[j] = neighbors.get(j);
			}
			point.setNearestNeighbors(knn);
			point.setLocalReachabilityDensity(Point.calculateLRD(point));
			if (KDTree.iskDistanceReady(tree, point, kDistance)) {
				point.setReady(true);
			} else {
				point.setReady(false);
			}
			// context.write(key, new Text(p.toString() + "#" + p.getID() + " "
			// + cellID));
			results.add(new Tuple2<Integer, Point>(arg0._1, point));
			neighbors.clear();
		}
		return results;
	}
}
