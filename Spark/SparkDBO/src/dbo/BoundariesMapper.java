package dbo;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class BoundariesMapper implements PairFunction<String,Integer,String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1061605503257488742L;

	@Override
	public Tuple2<Integer, String> call(String value) throws Exception {
		String valueAsString = value.toString();
		String pointAsString = valueAsString.substring(valueAsString
				.indexOf("\t") + 1);
		return new Tuple2<Integer, String>(0, pointAsString);
		
	}





	
	
}
