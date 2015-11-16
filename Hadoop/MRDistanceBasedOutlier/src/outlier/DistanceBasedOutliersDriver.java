package outlier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistanceBasedOutliersDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new DistanceBasedOutliersDriver(), args);
		System.exit(res);
	}

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
		String outputDir = args[1] + Path.SEPARATOR + "outliers";
		String epsilonString = args[2];
		String piString = args[3];
		int parallelization = Integer.parseInt(args[4]);
		conf.set("epsilon", epsilonString);
		conf.set("pi", piString);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(DistanceBasedOutliersDriver.class);
		job.setJobName("Density-based Outlier Detection");
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		job.setMapperClass(DistanceBasedOutliersMapper.class);
		job.setReducerClass(DistanceBasedOutliersReducer.class);
		job.setNumReduceTasks(parallelization);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
