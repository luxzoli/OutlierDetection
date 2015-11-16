package dbo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kdtree.Grid;
import kdtree.Point;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import util.BoundaryObject;

public class Driver {

	public static void main(String[] args) {
		long bTime = System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName(
				"SparkLOF");
		String inPath = args[0];
		String outPath = args[1];
		float epsilon = Float.parseFloat(args[2]);
		int pi = Integer.parseInt(args[3]);
		int sampleSize = Integer.parseInt(args[4]);
		int numPartitions = Integer.parseInt(args[5]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> pointStrings = sc.textFile(inPath);
		JavaPairRDD<Integer, Point> points = pointStrings
				.mapToPair(new PairFunction<String, Integer, Point>() {

					@Override
					public Tuple2<Integer, Point> call(String arg0)
							throws Exception {
						// System.out.println(arg0);
						return new Tuple2<Integer, Point>(0, new Point(arg0,
								true));
					}

				});
		Function<Point, BoundaryObject> createAcc = new Function<Point, BoundaryObject>() {
			public BoundaryObject call(Point p) {
				return new BoundaryObject(new Point(p), new Point(p));
			}
		};
		Function2<BoundaryObject, Point, BoundaryObject> addAndCount = new Function2<BoundaryObject, Point, BoundaryObject>() {
			public BoundaryObject call(BoundaryObject a, Point x) {
				float[] minA = a.getMin().getP();
				float[] maxA = a.getMax().getP();
				float[] p = x.getP();
				for (int d = 0; d < minA.length; d++) {
					minA[d] = p[d] < minA[d] ? p[d] : minA[d];
					maxA[d] = p[d] > maxA[d] ? p[d] : maxA[d];
				}
				return a;
			}
		};
		Function2<BoundaryObject, BoundaryObject, BoundaryObject> combine = new Function2<BoundaryObject, BoundaryObject, BoundaryObject>() {
			public BoundaryObject call(BoundaryObject a, BoundaryObject b) {
				float[] minA = a.getMin().getP();
				float[] maxA = a.getMax().getP();
				float[] minB = b.getMin().getP();
				float[] maxB = b.getMax().getP();
				for (int d = 0; d < minA.length; d++) {
					minA[d] = minB[d] < minA[d] ? minB[d] : minA[d];
					maxA[d] = maxB[d] > maxA[d] ? maxB[d] : maxA[d];
				}
				return a;
			}
		};
		Function2<BoundaryObject, BoundaryObject, BoundaryObject> func = new Function2<BoundaryObject, BoundaryObject, BoundaryObject>() {

			@Override
			public BoundaryObject call(BoundaryObject a, BoundaryObject b)
					throws Exception {

				float[] minA = a.getMin().getP();
				float[] maxA = a.getMax().getP();
				float[] minB = b.getMin().getP();
				float[] maxB = b.getMax().getP();
				for (int d = 0; d < minA.length; d++) {
					minA[d] = minB[d] < minA[d] ? minB[d] : minA[d];
					maxA[d] = maxB[d] > maxA[d] ? maxB[d] : maxA[d];
				}
				return a;
			}

		};
		BoundaryObject boundary = points
				.combineByKey(createAcc, addAndCount, combine)
				.reduceByKey(combine).first()._2();
		System.out.println(boundary);

		// TODO: argumentumból
		List<Tuple2<Integer, Point>> pointsTL = points.takeSample(false,
				sampleSize);
		Point[] pointsArr = new Point[pointsTL.size()];
		for (int i = 0; i < pointsArr.length; i++) {
			pointsArr[i] = pointsTL.get(i)._2;
		}
		int maxLeafSize = sampleSize / numPartitions;
		Grid grid = new Grid(pointsArr, maxLeafSize, boundary.getMin().getP(),
				boundary.getMax().getP());

		DistributePoints distributeF = new DistributePoints(grid);
		JavaPairRDD<Integer, Point> partitionedPoints = points
				.flatMapToPair(distributeF);
		//System.out.println(partitionedPoints.count());

		JavaPairRDD<Integer, Iterable<Point>> partitions = partitionedPoints
				.groupByKey();
		// TODO:k!!!
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> dbof = new DetectDBOutliers(
				pi, epsilon);
		JavaPairRDD<Integer, Point> outliers = partitions.flatMapToPair(dbof);
		JavaRDD<String> res = outliers
				.map(new Function<Tuple2<Integer, Point>, String>() {

					@Override
					public String call(Tuple2<Integer, Point> arg0)
							throws Exception {
						Point p = arg0._2;
						//System.out.println(new String(p.toSimpleString()));
						return new String(p.toSimpleString());
					}

				});
		// res.saveAsTextFile(outPath);
		List<String> lines = res.collect();
		long eTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (eTime - bTime) / 1000);
		File f = new File(outPath, "out.txt");
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(f))) {
			for (String line : lines) {
				bw.write(line + "\n");
			}
			bw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO: path
		// res.saveAsTextFile(outPath);

	}
}
