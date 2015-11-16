package lof;

import kdtree.Grid;
import kdtree.Point;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class DistributePoints implements
		PairFlatMapFunction<Tuple2<Integer, Point>, Integer, Point> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6566425207653423334L;
	private Grid grid;

	public DistributePoints(Grid grid) {
		this.grid = grid;
		//System.out.println(grid);
	}

	@Override
	public Iterable<Tuple2<Integer, Point>> call(Tuple2<Integer, Point> arg0)
			throws Exception {
		Point p = arg0._2;
		//System.out.println(grid);
		ArrayList<Grid> matching = grid.getMatchingGrids(p, 0.0f);
		List<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		for (Grid m : matching) {
			results.add(new Tuple2<Integer, Point>(new Integer(m.getID()), p));
		}
		return results;
	}

}
