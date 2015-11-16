package lof;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import kdtree.KDTree;
import kdtree.Point;
import scala.Tuple2;

public class KNNF2 implements
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> {
	private int k;

	public KNNF2(int k) {
		this.k = k;
	}

	@Override
	public Iterable<Tuple2<Integer, Point>> call(
			Tuple2<Integer, Iterable<Point>> arg0) throws Exception {
		ArrayList<Point> pointsOriginalAL = new ArrayList<Point>();
		ArrayList<Point> pointsGuestAL = new ArrayList<Point>();
		int cellID = arg0._1;
		for (Point p : arg0._2) {
			if (p.getCellID() == cellID) {
				pointsOriginalAL.add(p);
			} else {
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
		KDTree tree = new KDTree(pointsO, 10);
		ArrayList<Point> neighbors = new ArrayList<Point>();
		for (Point p : pointsG) {
			float kDistance = KDTree.kNNQuery(tree, p, k, neighbors);
			p.setkDistance(kDistance);
			p.setReady(false);
			Point[] knn = new Point[neighbors.size()];
			for (int j = 0; j < neighbors.size(); j++) {
				knn[j] = neighbors.get(j);
			}
			p.setNearestNeighbors(knn);
			// context.write(key, new Text(p.toString() + "#" + p.getID() + " "
			// + p.getCellID()));
			neighbors.clear();
		}
		pointsOriginalAL = null;
		ArrayList<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		for (Point p : pointsGuestAL) {
			results.add(new Tuple2<Integer, Point>(p.getCellID(), p));
		}
		return results;
	}
}
