package outlier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LOFDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new LOFDriver(), args);
		System.exit(res);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {

		//if (args.length != 5) {
		//	System.out
		//			.printf("Usage: DensityBasedOutliersDriver <input dir> <output dir> <epsilon> <pi> <parallelization>\n");
		//	System.exit(-1);
		//}
		
		System.out.printf("Here!");
		Configuration conf = new Configuration();
		String inputDir = args[0] + Path.SEPARATOR + "partitions";
		String outputDir1 = args[1] + Path.SEPARATOR + "1";
		String outputDir2 = args[1] + Path.SEPARATOR + "2";
		String outputDir3 = args[1] + Path.SEPARATOR + "3";
		String outputDir4 = args[1] + Path.SEPARATOR + "4";
		String outputDir5 = args[1] + Path.SEPARATOR + "5";
		String outputDir6 = args[1] + Path.SEPARATOR + "6";
		String outputDir7 = args[1] + Path.SEPARATOR + "7";
		String outputDir8 = args[1] + Path.SEPARATOR + "8";
		String outputDir9 = args[1] + Path.SEPARATOR + "9";
		String outputDir10 = args[1] + Path.SEPARATOR + "LOFScores";
		String gridPath = args[0] + Path.SEPARATOR + "grid";
		Path gridResPath = new Path(gridPath);
		String gridAsString = readGrid(gridResPath);
		String epsilonString = args[2];
		String piString = args[3];
		int parallelization = Integer.parseInt(args[4]);
		conf = new Configuration();
		conf.set("epsilon", epsilonString);
		conf.set("pi", piString);
		
		//1 k-Distance
		Job job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Calculating k-Distance 1");
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir1));
		job.setMapperClass(KDistancesFirstMapper.class);
		job.setReducerClass(KDistancesFirstReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		//2 k-Distance
		conf = new Configuration();
		conf.set("epsilon", epsilonString);
		conf.set("pi", piString);
		conf.set("gridAsString",gridAsString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Calculating k-Distance 2");
		FileInputFormat.setInputPaths(job, new Path(outputDir1));
		FileOutputFormat.setOutputPath(job, new Path(outputDir2));
		job.setMapperClass(KDistancesSecondMapper.class);
		job.setReducerClass(KDistancesSecondReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		success &= job.waitForCompletion(true);
		
		//filter
		conf = new Configuration();
		conf.set("pi", piString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Filtering k-Distance 2");
		FileInputFormat.setInputPaths(job,new Path(outputDir1), new Path(outputDir2));
		FileOutputFormat.setOutputPath(job, new Path(outputDir3));
		job.setMapperClass(FilterKDistancesMapper.class);
		job.setReducerClass(FilterKDistancesReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		success &= job.waitForCompletion(true);
		
		//3 LRD 1
		conf = new Configuration();
		conf.set("pi", piString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Calculating LRD 1");
		FileInputFormat.setInputPaths(job,new Path(outputDir1), new Path(outputDir3));
		FileOutputFormat.setOutputPath(job, new Path(outputDir4));
		job.setMapperClass(LRDFirstMapper.class);
		job.setReducerClass(LRDFirstReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		success &= job.waitForCompletion(true);
		
		//4 LRD 2
		conf = new Configuration();
		conf.set("pi", piString);
		conf.set("gridAsString",gridAsString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Calculating LRD 2");
		FileInputFormat.setInputPaths(job, new Path(outputDir4));
		FileOutputFormat.setOutputPath(job, new Path(outputDir5));
		job.setMapperClass(LRDSecondMapper.class);
		job.setReducerClass(LRDSecondReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		success &= job.waitForCompletion(true);
		//Filter LRD 2
		conf = new Configuration();
		conf.set("pi", piString);
		conf.set("gridAsString",gridAsString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Filtering LRD");
		FileInputFormat.setInputPaths(job,new Path(outputDir4), new Path(outputDir5));
		FileOutputFormat.setOutputPath(job, new Path(outputDir6));
		job.setMapperClass(FilterLRDMapper.class);
		job.setReducerClass(FilterLRDReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		success &= job.waitForCompletion(true);
		
		//5 LOF 1
		conf = new Configuration();
		conf.set("pi", piString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Calculating LOF 1");
		FileInputFormat.setInputPaths(job,new Path(outputDir4), new Path(outputDir6));
		FileOutputFormat.setOutputPath(job, new Path(outputDir7));
		job.setMapperClass(LOFFirstMapper.class);
		job.setReducerClass(LOFFirstReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		success &= job.waitForCompletion(true);
		
		//6 LOF 2
		conf = new Configuration();
		conf.set("pi", piString);
		conf.set("gridAsString",gridAsString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Calculating LOF 2");
		FileInputFormat.setInputPaths(job, new Path(outputDir7));
		FileOutputFormat.setOutputPath(job, new Path(outputDir8));
		job.setMapperClass(LOFSecondMapper.class);
		job.setReducerClass(LOFSecondReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		success &= job.waitForCompletion(true);
		
		//Filtering LOF 2
		conf = new Configuration();
		conf.set("pi", piString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Filtering LOF");
		FileInputFormat.setInputPaths(job, new Path(outputDir7), new Path(outputDir8));
		FileOutputFormat.setOutputPath(job, new Path(outputDir9));
		job.setMapperClass(FilterLOFMapper.class);
		job.setReducerClass(FilterLOFReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		success &= job.waitForCompletion(true);
		//Collecting results
		conf = new Configuration();
		conf.set("pi", piString);
		job = new Job(conf);
		job.setJarByClass(LOFDriver.class);
		job.setJobName("Collecting results");
		FileInputFormat.setInputPaths(job, new Path(outputDir7),new Path(outputDir9));
		FileOutputFormat.setOutputPath(job, new Path(outputDir10));
		job.setMapperClass(CollectLOFScoresMapper.class);
		job.setReducerClass(CollectLOFScoresReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		success &= job.waitForCompletion(true);
		return success ? 0 : 1;
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
