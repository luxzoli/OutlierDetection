package lof;

import kdtree.Grid;

import java.util.ArrayList;

import kdtree.Point;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class KNNF2FlatMap implements
		PairFlatMapFunction<Tuple2<Integer, Point>, Integer, Point> {
	private Grid grid;

	public KNNF2FlatMap(Grid grid) {
		this.grid = grid;
	}

	@Override
	public Iterable<Tuple2<Integer, Point>> call(Tuple2<Integer, Point> arg0)
			throws Exception {
		ArrayList<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		Point p = arg0._2;
		if (!p.isReady()) {
			ArrayList<Grid> matching = grid.getMatchingGrids(p,
					p.getkDistance());
			for (Grid m : matching) {
				// lehet fölösleges
				p.setCellID(arg0._1);
				results.add(new Tuple2<Integer, Point>(m.getID(), p));
			}
		}else {
			results.add(new Tuple2<Integer, Point>(p.getCellID(), p));
		}	
		return results;
	}

}
