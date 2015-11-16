package outlier;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CollectLOFScoresMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	private Text outValue = new Text();
	private IntWritable outKey = new IntWritable();
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueAsString = value.toString();
		int ID = Integer.parseInt(valueAsString.substring(0,
				valueAsString.indexOf("\t")));
		 outKey.set(ID);
		String pointAsString = valueAsString.substring(valueAsString
				.indexOf("\t") + 1);
		StringTokenizer st = new StringTokenizer(pointAsString, "#");
		pointAsString = st.nextToken();
		//String IDString = st.nextToken();
		if(pointAsString.endsWith("r")){
			outValue.set(pointAsString);
			context.write(outKey, outValue);
		}
	}
}
