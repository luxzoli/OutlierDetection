package grid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class GridDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GridDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// if (args.length != 6) {
		// System.out
		// .printf("Usage: GridDriver <input dir> <output dir> <sample size> <max leaf size> <epsilon> <parallelization>\n");
		// System.exit(-1);
		// }
		String inputDir = args[0];
		String outputDir = args[1];
		String sampleSize = args[2];
		String maxLeafSize = args[3];
		String epsilon = args[4];
		int parallelization = Integer.parseInt(args[5]);
		String boundariesPath = outputDir + Path.SEPARATOR + "boundaries";
		String gridPath = outputDir + Path.SEPARATOR + "grid";
		String partitionedDataPath = outputDir + Path.SEPARATOR + "partitions";
		String analysisDataPath = outputDir + Path.SEPARATOR + "analysis";
		Configuration conf = this.getConf();
		Job job = new Job(conf);
		job.setJarByClass(GridDriver.class);
		job.setJobName("Calculate Boundaries");

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(boundariesPath));

		job.setMapperClass(BoundariesMapper.class);
		job.setCombinerClass(BoundariesCombiner.class);
		job.setReducerClass(BoundariesReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);

		conf = new Configuration();

		Path boundariesResPath = new Path(boundariesPath);
		String[] boundaries = readBoundaries(boundariesResPath);
		String lowerBoundary = boundaries[0];
		String upperBoundary = boundaries[1];
		String numPoints = boundaries[2];
		conf.set("sampleSize", sampleSize);
		conf.set("maxLeafSize", maxLeafSize);
		conf.set("lowerBoundary", lowerBoundary);
		conf.set("upperBoundary", upperBoundary);
		conf.set("numPoints", numPoints);
		job = new Job(conf);
		job.setJarByClass(GridDriver.class);
		job.setJobName("Construct Grid with Sampling");

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(gridPath));

		job.setMapperClass(GridMapper.class);
		job.setCombinerClass(GridCombiner.class);
		job.setReducerClass(GridReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		success &= job.waitForCompletion(true);
		conf = new Configuration();
		Path gridResPath = new Path(gridPath);
		// String gridAsString =
		// readGrid(gridResPath.getFileSystem(conf),gridResPath);
		String gridAsString = readGrid(gridResPath);
		conf.set("gridAsString", gridAsString);
		conf.set("epsilon", epsilon);
		job = new Job(conf);
		job.setJarByClass(GridDriver.class);
		job.setJobName("Distribute Points into the Grid");
		job.setNumReduceTasks(parallelization);
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(partitionedDataPath));

		job.setMapperClass(DistributePointsMapper.class);
		job.setReducerClass(DistributePointsReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		success &= job.waitForCompletion(true);

		conf = new Configuration();
		conf.set("gridAsString", gridAsString);
		conf.set("epsilon", epsilon);
		job = new Job(conf);
		job.setJarByClass(GridDriver.class);
		job.setJobName("Analyze the Grid");

		FileInputFormat.setInputPaths(job, new Path(partitionedDataPath));
		FileOutputFormat.setOutputPath(job, new Path(analysisDataPath));

		job.setMapperClass(AnalyzeGridMapper.class);
		job.setCombinerClass(AnalyzeGridReducer.class);
		job.setReducerClass(AnalyzeGridReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		success &= job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static String[] readBoundaries(Path pt) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(pt, false);
		String lowerBoundary = null;
		String upperBoundary = null;
		String numPoints = null;
		while (fileIter.hasNext()) {
			LocatedFileStatus lfs = fileIter.next();
			Path pathToOpen = lfs.getPath();
			// System.out.println("here");
			// System.out.println(pathToOpen);
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pathToOpen)))) {
				String line = br.readLine();
				if (line != null) {
					String line2 = br.readLine();
					String line3 = br.readLine();
					// System.out.println(line1);
					// System.out.println(line2);
					lowerBoundary = new String(line.substring(line
							.indexOf("\t") + 1));
					upperBoundary = new String(line2.substring(line2
							.indexOf("\t") + 1));
					numPoints = new String(
							line3.substring(line3.indexOf("\t") + 1));
				} else {
					continue;
				}
			}
		}
		return new String[] { lowerBoundary, upperBoundary, numPoints };
	}

	public static String readGrid(Path pt) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(pt, false);
		String grid = null;
		while (fileIter.hasNext()) {
			LocatedFileStatus lfs = fileIter.next();
			Path pathToOpen = lfs.getPath();
			// System.out.println("here");
			// System.out.println(pathToOpen);
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pathToOpen)))) {
				String line = br.readLine();
				if (line != null) {
					grid = new String(line.substring(line.indexOf("\t") + 1));
				} else {
					continue;
				}
			}
		}
		return grid;
	}

}
