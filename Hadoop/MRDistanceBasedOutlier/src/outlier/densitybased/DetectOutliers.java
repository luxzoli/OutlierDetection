package outlier.densitybased;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import outlier.DensityBasedOutliersDriver;
import grid.GridDriver;

public class DetectOutliers {

	public static void main(String[] args) throws Exception {
		// if (args.length != 7) {
		// System.out
		// .printf("Usage: <input dir> <output dir> <epsilon> <pi> <sample size> <min cell count> <parallelizetion>\n");
		// System.exit(-1);
		// }
		System.out
				.printf("Usage: <input dir> <output dir> <epsilon> <pi> <sample size> <min cell count> <parallelizetion>\n");
		String inputDir = args[0];
		String outputDir = args[1];
		String epsilon = args[2];
		String pi = args[3];
		String sampleSize = args[4];
		int sampleSizeInt = Integer.parseInt(args[4]);
		int minCellCount = Integer.parseInt(args[5]);
		String parallelizetion = args[6];
		String maxSampleLeafSize = new String((sampleSizeInt / minCellCount)
				+ "");
		String[] gargs = new String[args.length - 1];
		gargs[0] = inputDir;
		gargs[1] = outputDir;
		gargs[2] = sampleSize;
		gargs[3] = maxSampleLeafSize;
		gargs[4] = epsilon;
		gargs[5] = parallelizetion;
		for (int i = 7; i < args.length; i++) {
			gargs[i - 1] = args[i];
		}
		int res = ToolRunner.run(new Configuration(), new GridDriver(), gargs);
		if (res != 0) {
			System.exit(res);
		}
		String[] oargs = new String[args.length - 2];
		oargs[0] = outputDir;
		oargs[1] = outputDir;
		oargs[2] = epsilon;
		oargs[3] = pi;
		oargs[4] = parallelizetion;
		for (int i = 7; i < args.length; i++) {
			oargs[i - 2] = args[i];
		}
		res = ToolRunner.run(new Configuration(),
				new DensityBasedOutliersDriver(), oargs);
		System.exit(res);

	}

}
