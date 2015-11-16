package grid;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@SuppressWarnings("unused")
public class BoundariesReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Configuration conf = context.getConfiguration();
		long numberOfPoints = 0;
		Iterator<Text> iter = values.iterator();
		String firstValue = iter.next().toString();
		firstValue = firstValue.substring(firstValue.indexOf("\t") + 1);
		while (firstValue.startsWith("num")) {
			numberOfPoints += Long.parseLong(firstValue.substring(3));
			firstValue = iter.next().toString();
			firstValue = firstValue.substring(firstValue.indexOf("\t") + 1);
		}
		// spatial boundary values from the combiner end with c
		firstValue = firstValue.substring(0, firstValue.length() - 1);
		StringTokenizer fst = new StringTokenizer(firstValue, " ");
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
			Text value = iter.next();
			// TODO:if d and the number of tokens do not match, there is a
			// problem
			String valueString = value.toString();
			valueString = valueString.substring(valueString.indexOf("\t") + 1);
			if (valueString.startsWith("num")) {
				numberOfPoints += Long.parseLong(valueString.substring(3));
				continue;
			}
			// spatial boundary values from the combiner end with c
			valueString = valueString.substring(0, valueString.length() - 1);
			if (valueString == "") {
				continue;
			}
			StringTokenizer st = new StringTokenizer(
					valueString.substring(valueString.indexOf("\t") + 1), " ");
			// ID nem kell
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
		context.write(key, new Text(lowerBoundaryString));
		context.write(key, new Text(upperBoundaryString));
		context.write(key, new Text(numberOfPoints + ""));
	}
}