package outlier;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterLOFMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outValue = new Text();

	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueAsString = value.toString();
		String pointAsString = valueAsString.substring(valueAsString
				.indexOf("\t") + 1);
		StringTokenizer st = new StringTokenizer(pointAsString,"#");
		outValue.set(st.nextToken());
		outKey.set(st.nextToken());
		if (outValue.toString().endsWith("n")) {
			context.write(outKey, outValue);
		}
	}
}

