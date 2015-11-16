package grid;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import point.Grid;
import point.Point;

public class GridReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	private static Random random;
	private static PriorityQueue<Entry> queue;
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int sampleSize = Integer.parseInt(conf.get("sampleSize"));
		String lowerBoundaryString = conf.get("lowerBoundary");
		String upperBoundaryString = conf.get("upperBoundary");
		int maxLeafSize = Integer.parseInt(conf.get("maxLeafSize"));
		//int numSamples = Integer.parseInt(conf.get("numSamples"));
		random = new Random(new Date().getTime());
		 Comparator<Entry> comparator = new EntryComparator();
			queue = new PriorityQueue<Entry>((sampleSize),comparator);
		for (Text value : values) {
			double priority = random.nextDouble();
			if(queue.size() >= (sampleSize)){
				queue.poll();
			}
			Entry entry = new Entry(new Text(value.toString()),priority);
			queue.add(entry);
		}
		int size = queue.size();
		ArrayList<Point> points = new ArrayList<Point>();
		for(int i = 0; i < (size);i++){
			Entry entry = queue.poll();
			//System.out.println((key));
			String s = new String(entry.getValue().toString());
			Point p = new Point(s);
			points.add(p);
		}
		Point[] pointsArr = new Point[points.size()];
		int k = 0;
		for(Point p : points){
			pointsArr[k] = p;
			k++;
		}
		points.clear();
		System.out.println(lowerBoundaryString);
		StringTokenizer st = new StringTokenizer(lowerBoundaryString," ");
		int d = st.countTokens();
		float[] lowerBoundary = new float[d];
		d = st.countTokens();
		for(int i = 0; i < d; i++){
			lowerBoundary[i] = Float.parseFloat(st.nextToken());
		}
		st = new StringTokenizer(upperBoundaryString," ");
		float[] upperBoundary = new float[d];
		for(int i = 0; i < d; i++){
			upperBoundary[i] = Float.parseFloat(st.nextToken());
		}
		Grid grid = new Grid(pointsArr,maxLeafSize, lowerBoundary, upperBoundary);
		Text gridAsText = new Text(grid.toString());
		context.write(key, gridAsText );
		// System.out.println(outputString);
	}
	
	

}