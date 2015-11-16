package dbo;

import grid.Point;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class BoundariesPairFlatMap implements
		PairFlatMapFunction<Tuple2<Integer, Iterable<String>>, Integer, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2343455676228742176L;

	@Override
	public Iterable<Tuple2<Integer, String>> call(
			Tuple2<Integer, Iterable<String>> iterator) throws Exception {
		long numberOfPoints = 0;
		Iterator<String> iter = iterator._2.iterator();
		String firstValue = iter.next() ;
		firstValue = firstValue.substring(firstValue.indexOf("\t") + 1);
		while (firstValue.startsWith("num")) {
			numberOfPoints += Long.parseLong(firstValue.substring(3));
			firstValue = iter.next().toString();
			firstValue = firstValue.substring(firstValue.indexOf("\t") + 1);
		}
		if (firstValue.endsWith("c")) {
			firstValue = firstValue.substring(0, firstValue.length()- 1);
		} else {
			numberOfPoints++;
		}
		StringTokenizer fst = new StringTokenizer(firstValue, " ");
		//ID nem kell
		if (fst.hasMoreTokens()) {
			fst.nextToken();
		}
		int d = fst.countTokens();
		float[] lowerBoundary = new float[d];
		float[] upperBoundary = new float[d];
		for (int i = 0; i < d; i++) {
			lowerBoundary[i] = Float.MAX_VALUE;
			upperBoundary[i] = Float.MIN_VALUE;
		}
		for (int i = 0; i < d; i++) {
			float currCoord = Float.parseFloat(fst.nextToken());
			if (currCoord > upperBoundary[i]) {
				upperBoundary[i] = currCoord;
			}
			if (currCoord < lowerBoundary[i]) {
				lowerBoundary[i] = currCoord;
			}
		}
		while (iter.hasNext()) {
			String value = iter.next();
			// TODO:ha d és tokenek nem passzolnak baj van
			String valueString = value.substring(value.indexOf("\t") + 1);
			if (valueString.startsWith("num")) {
				numberOfPoints += Long.parseLong(valueString.substring(3));
				continue;
			}
			if (valueString == "") {
				continue;
			}
			if (valueString.endsWith("c")) {
				valueString = valueString.substring(0, valueString.length()- 1);
			} else {
				numberOfPoints++;
			}
			StringTokenizer st = new StringTokenizer(
					valueString.substring(valueString.indexOf("\t") + 1), " ");
			//ID nem kell
			if (st.hasMoreTokens()) {
				st.nextToken();
			}
			for (int i = 0; i < d; i++) {
				float currCoord = Float.parseFloat(st.nextToken());
				if (currCoord > upperBoundary[i]) {
					upperBoundary[i] = currCoord;
				}
				if (currCoord < lowerBoundary[i]) {
					lowerBoundary[i] = currCoord;
				}
			}
		}
		String lowerBoundaryString = new String();
		String upperBoundaryString = new String();
		for (int i = 0; i < d; i++) {
			lowerBoundaryString += lowerBoundary[i] + " ";
			upperBoundaryString += upperBoundary[i] + " ";
		}
		lowerBoundaryString = lowerBoundaryString.replace(' ', ' ');
		upperBoundaryString = upperBoundaryString.replace(' ', ' ');
		List<Tuple2<Integer, String>> results = new ArrayList<Tuple2<Integer, String>>();
		results.add(new Tuple2<Integer,String>(0, new String(lowerBoundaryString + "c")));
		results.add(new Tuple2<Integer,String>(0, new String(upperBoundaryString + "c")));
		results.add(new Tuple2<Integer,String>(0, new String("num" + numberOfPoints)));
		return results;
	}
}
