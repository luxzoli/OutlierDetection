package dbo;

import java.util.ArrayList;

import kdtree.KDTree;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import kdtree.Point;

public class DetectDBOutliers implements
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> {
	private int pi;
	private float epsilon;

	public DetectDBOutliers(int pi, float epsilon) {
		this.pi = pi;
		this.epsilon = epsilon;
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
			//
			points[i] = point;
			i++;
		}
		pointsAL = null;
		KDTree tree = new KDTree(points, 10);

		ArrayList<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		for (Point point : points) {
			float kDistance = KDTree.kDistance(tree, point, pi);
			if (point.isCorePoint()) {
				if (kDistance > epsilon) {
					results.add(new Tuple2<Integer, Point>(arg0._1, point));
				}
			}

		}
		return results;
	}
}
