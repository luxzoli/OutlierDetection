package lof;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import util.BoundaryObject;

public class Driver {

	public static void main(String[] args) {
		long bTime = System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkLOF")
				.set("spark.executor.memory", "8g").set("spark.driver.memory", "8g");// .set("spark.driver.cores",
																						// "4")
		// .set("spark.kryoserializer.buffer.max", "1g");
		String inPath = args[0];
		String outPath = args[1];
		int k = Integer.parseInt(args[2]);
		int sampleSize = Integer.parseInt(args[3]);
		int numPartitions = Integer.parseInt(args[4]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> pointStrings = sc.textFile(inPath);
		JavaPairRDD<Integer, Point> points = pointStrings.mapToPair(new PairFunction<String, Integer, Point>() {

			@Override
			public Tuple2<Integer, Point> call(String arg0) throws Exception {
				// System.out.println(arg0);
				return new Tuple2<Integer, Point>(0, new Point(arg0, true));
			}

		}).persist(StorageLevel.DISK_ONLY());
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
				a.setCount(a.getCount() + 1);
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
				a.setCount(a.getCount() + b.getCount());
				return a;
			}
		};
		Function2<BoundaryObject, BoundaryObject, BoundaryObject> func = new Function2<BoundaryObject, BoundaryObject, BoundaryObject>() {

			@Override
			public BoundaryObject call(BoundaryObject a, BoundaryObject b) throws Exception {

				float[] minA = a.getMin().getP();
				float[] maxA = a.getMax().getP();
				float[] minB = b.getMin().getP();
				float[] maxB = b.getMax().getP();
				for (int d = 0; d < minA.length; d++) {
					minA[d] = minB[d] < minA[d] ? minB[d] : minA[d];
					maxA[d] = maxB[d] > maxA[d] ? maxB[d] : maxA[d];
				}
				a.setCount(a.getCount() + b.getCount());
				return a;
			}

		};
		BoundaryObject boundary = points.combineByKey(createAcc, addAndCount, combine).reduceByKey(combine).first()
				._2();
		System.out.println(boundary);

		List<Tuple2<Integer, Point>> pointsTL = points.takeSample(false, sampleSize);
		Point[] pointsArr = new Point[pointsTL.size()];
		for (int i = 0; i < pointsArr.length; i++) {
			pointsArr[i] = pointsTL.get(i)._2;
		}
		int maxLeafSize = sampleSize / numPartitions;
		Grid grid = new Grid(pointsArr, maxLeafSize, boundary.getMin().getP(), boundary.getMax().getP());
		pointsArr = null;
		pointsTL = null;
		DistributePoints distributeF = new DistributePoints(grid);
		JavaPairRDD<Integer, Point> partitionedPoints = points.flatMapToPair(distributeF)
				.persist(StorageLevel.DISK_ONLY());
		// System.out.println("points count " + partitionedPoints.count());
		//TODO:
		//JavaPairRDD<Integer, Iterable<Point>> partitions = partitionedPoints.groupByKey()
		//		.persist(StorageLevel.DISK_ONLY());
		
		// partitions.count();
		// partitions.cache();
		// pointStrings = null;
		// partitionedPoints = null;
		//partitions.count();
		pointStrings.unpersist();
		partitionedPoints.unpersist();
		points.unpersist();
		// -----------------------
		List<Tuple2<Integer, Point>> pres = partitionedPoints.collect();
		sc.stop();
		sc = new JavaSparkContext(sparkConf);
		JavaPairRDD<Integer, Iterable<Point>> partitions = sc.parallelizePairs(pres).groupByKey().persist(StorageLevel.DISK_ONLY());
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> knnF1 = new KNNF1(k);
		JavaPairRDD<Integer, Point> knn1Res = partitions.flatMapToPair(knnF1)
				.persist(StorageLevel.DISK_ONLY());
		// System.out.println("knn1 res count " + knn1Res.count());
		// knn1Res.cache();
		// partitions = null;
		PairFlatMapFunction<Tuple2<Integer, Point>, Integer, Point> notReadyMap = new KNNF2FlatMap(grid);

		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> knnF2 = new KNNF2(k);
		// JavaPairRDD<Integer, Point> knn1NR =
		// knn1Res.flatMapToPair(notReadyMap);
		// System.out.println("knn1 nr count " + knn1NR.count());
		JavaPairRDD<Integer, Point> knn2Res = knn1Res.flatMapToPair(notReadyMap).groupByKey().flatMapToPair(knnF2)
				.persist(StorageLevel.DISK_ONLY());
		// JavaPairRDD<Integer, Iterable<Point>> partitions2 =
		// partitionedPoints.groupByKey();
		// System.out.println("p2 nr count " + partitions2.count());
		// JavaPairRDD<Integer, Iterable<Point>> knn2RT = knn1NR.groupByKey();
		// knn2RT.count();
		// JavaPairRDD<Integer, Point> knn2Res = knn2RT.flatMapToPair(knnF2);
		Function<Tuple2<Integer, Point>, Boolean> filterNotReady = new Function<Tuple2<Integer, Point>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Integer, Point> arg0) throws Exception {
				// System.out.println(arg0._2.isReady());
				return !arg0._2.isReady();
			}

		};
		// System.out.println("knn2 res count " + knn2Res.count());
		// knn2Res.cache();
		JavaPairRDD<Integer, Point> knn3Temp = knn1Res.union(knn2Res).filter(filterNotReady)
				.persist(StorageLevel.DISK_ONLY());
		PairFunction<Tuple2<Long, Iterable<Point>>, Integer, Point> knnF3 = new KNNF3(k);
		PairFunction<Tuple2<Integer, Point>, Long, Point> IDF = new PairFunction<Tuple2<Integer, Point>, Long, Point>() {
			@Override
			public Tuple2<Long, Point> call(Tuple2<Integer, Point> arg0) throws Exception {
				Point p = arg0._2;
				return new Tuple2<Long, Point>(p.getID(), p);
			}
		};
		JavaPairRDD<Integer, Point> knn3Res = knn3Temp.mapToPair(IDF).groupByKey().mapToPair(knnF3)
				.persist(StorageLevel.DISK_ONLY());

		Function<Tuple2<Integer, Point>, Boolean> filterReady = new Function<Tuple2<Integer, Point>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Integer, Point> arg0) throws Exception {

				return arg0._2.isReady();
			}

		};

		JavaPairRDD<Integer, Point> knns = knn1Res.filter(filterReady).union(knn3Res)
				.persist(StorageLevel.DISK_ONLY());
		// System.out.println("knns count " + knns.count());
		//TODO:
		//JavaPairRDD<Integer, Iterable<Point>> pKNNS = knns.groupByKey().persist(StorageLevel.DISK_ONLY());
		//pKNNS.count();
		// knns = null;
		// knn3Res = null;
		// knn3Temp = null;
		// knn2Res = null;
		// knn1Res = null;
		//partitions.unpersist();
		//knns.unpersist();
		//knn3Res.unpersist();
		//knn3Temp.unpersist();
		//knn2Res.unpersist();
		//knn1Res.unpersist();
		//-----------------------
		pres.clear();
		pres = knns.collect();
		sc.stop();
		sc = new JavaSparkContext(sparkConf);
		JavaPairRDD<Integer, Iterable<Point>> pKNNS = sc.parallelizePairs(pres).groupByKey().persist(StorageLevel.DISK_ONLY());
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> LRDF1 = new LRDF1(k);
		JavaPairRDD<Integer, Point> LRD1Res = pKNNS.flatMapToPair(LRDF1).persist(StorageLevel.DISK_ONLY());
		// LRD1Res.count();
		// LRD1Res.cache();

		// TODO:k!!!
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> LRDF2 = new LRDF2(k);
		JavaPairRDD<Integer, Point> LRD2Res = LRD1Res.flatMapToPair(notReadyMap).groupByKey().flatMapToPair(LRDF2)
				.persist(StorageLevel.DISK_ONLY());
		// LRD2Res.count();
		// LRD2Res.cache();
		JavaPairRDD<Integer, Point> LRD3Temp = LRD1Res.union(LRD2Res).filter(filterNotReady)
				.persist(StorageLevel.DISK_ONLY());
		PairFunction<Tuple2<Long, Iterable<Point>>, Integer, Point> LRDF3 = new LRDF3(k);
		JavaPairRDD<Integer, Point> LRD3Res = LRD3Temp.mapToPair(IDF).groupByKey().mapToPair(LRDF3)
				.persist(StorageLevel.DISK_ONLY());
		// LRD3Res.count();
		// LRD3Res.cache();
		JavaPairRDD<Integer, Point> LRDS = LRD1Res.filter(filterReady).union(LRD3Res)
				.persist(StorageLevel.DISK_ONLY());
		// System.out.println("lrds count " + LRDS.count());

		//JavaPairRDD<Integer, Iterable<Point>> pLRDS = LRDS.groupByKey().persist(StorageLevel.DISK_ONLY());
		//pLRDS.count();
		// pLRDS.cache();
		// LRDS = null;
		// LRD1Res = null;
		// LRD2Res = null;
		// LRD3Res = null;
		// LRD3Temp = null;
		//pKNNS.unpersist();
		//LRDS.unpersist();
		//LRD1Res.unpersist();
		//LRD2Res.unpersist();
		//LRD3Res.unpersist();
		//LRD3Temp.unpersist();
		// -----------------------
		pres.clear();
		pres = LRDS.collect();
		sc.stop();
		sc = new JavaSparkContext(sparkConf);
		JavaPairRDD<Integer, Iterable<Point>> pLRDS = sc.parallelizePairs(pres).groupByKey().persist(StorageLevel.DISK_ONLY());
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> LOFF1 = new LOFF1(k);
		JavaPairRDD<Integer, Point> LOF1Res = pLRDS.flatMapToPair(LOFF1).persist(StorageLevel.DISK_ONLY());
		// pLRDS = null;
		// LOF1Res.count();
		// LOF1Res.cache();
		// TODO:k!!!
		PairFlatMapFunction<Tuple2<Integer, Iterable<Point>>, Integer, Point> LOFF2 = new LOFF2(k);
		JavaPairRDD<Integer, Point> LOF2Res = LOF1Res.flatMapToPair(notReadyMap).groupByKey().flatMapToPair(LOFF2)
				.persist(StorageLevel.DISK_ONLY());
		// LOF2Res.count();
		// LOF2Res.cache();
		JavaPairRDD<Integer, Point> LOF3Temp = LOF1Res.union(LOF2Res).filter(filterNotReady)
				.persist(StorageLevel.DISK_ONLY());
		PairFunction<Tuple2<Long, Iterable<Point>>, Integer, Point> LOFF3 = new LOFF3(k);
		JavaPairRDD<Integer, Point> LOF3Res = LOF3Temp.mapToPair(IDF).groupByKey().mapToPair(LOFF3)
				.persist(StorageLevel.DISK_ONLY());
		// LOF3Res.count();
		// LOF3Res.cache();
		JavaPairRDD<Integer, Point> LOFs = LOF1Res.filter(filterReady).union(LOF3Res)
				.persist(StorageLevel.DISK_ONLY());
		// LOFs.cache();
		// System.out.println("lofs count " + LOFs.count());
		// -----------------------
		// LOF1Res = null;
		// LOF2Res = null;
		// LOF3Temp = null;
		// LOF3Res = null;
		//LOFs.count();
		//pLRDS.unpersist();
		//LOF1Res.unpersist();
		//LOF2Res.unpersist();
		//LOF3Temp.unpersist();
		//LOF3Res.unpersist();
		JavaRDD<String> res = LOFs.map(new Function<Tuple2<Integer, Point>, String>() {

			@Override
			public String call(Tuple2<Integer, Point> arg0) throws Exception {
				Point p = arg0._2;
				// System.out.println(new String(p.toSimpleString()
				// + " Score: " + p.getLOFScore()));
				return new String(p.toSimpleString() + " " + p.getLOFScore());
			}

		});
		// System.out.println("lofs res count " + res.count());

		List<String> lines = res.collect();
		pres.clear();
		long eTime = System.currentTimeMillis();
		System.out.println("Execution time: " + (eTime - bTime) / 1000);
		LOFs = null;
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
			e.printStackTrace();
		}
		sc.stop();
		// TODO: save (not windows)
		// res.saveAsTextFile(outPath);

	}
}
