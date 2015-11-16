package grid;

import org.apache.hadoop.io.Text;

public class Entry {
	private Text value;
	private Double priority;
	
	public Entry(Text value, Double priority) {
		this.value = value;
		this.priority = priority;
	}
	
	public Text getValue() {
		return value;
	}

	public Double getPriority() {
		return priority;
	}

	@Override
	public String toString() {
		return "Entry [value=" + value + ", priority=" + priority + "]";
	}
	
}
