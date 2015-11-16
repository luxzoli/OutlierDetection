package grid;

import java.util.Comparator;

public class EntryComparator implements Comparator<Entry> {

	@Override
	public int compare(Entry o1, Entry o2) {
		return o1.getPriority().compareTo(o2.getPriority());
	}

}
