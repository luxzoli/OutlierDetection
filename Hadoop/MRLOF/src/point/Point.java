package point;

import java.util.Arrays;
import java.util.StringTokenizer;

public class Point {
	// private int d;
	private float[] p;

	// private boolean isCorePoint;
	public Point(float[] p) {
		// this.d = p.length;
		this.p = p;
	}

	public Point(String p) {
		StringTokenizer st = new StringTokenizer(p);
		int d = st.countTokens();
		this.p = new float[d];
		for (int i = 0; st.hasMoreTokens(); i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
	}

	public Point(String p, String delim) {
		StringTokenizer st = new StringTokenizer(p, delim);
		int d = st.countTokens();
		this.p = new float[d];
		for (int i = 0; st.hasMoreTokens(); i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
	}

	public static float euclideanDistance(Point p1, Point p2) {
		/*
		 * if(o.getD() != this.d){ return null; }
		 */
		float distance = 0;
		for (int i = 0; i < p1.p.length; i++) {
			distance += ((p1.p[i] - p2.getP()[i]) * (p1.p[i] - p2.p[i]));
		}
		distance = (float) Math.sqrt(distance);
		return distance;
	}
	
	public static float euclideanDistance(float[] p1, Point p2) {
		/*
		 * if(o.getD() != this.d){ return null; }
		 */
		float distance = 0;
		for (int i = 0; i < p1.length; i++) {
			distance += ((p1[i] - p2.getP()[i]) * (p1[i] - p2.p[i]));
		}
		distance = (float) Math.sqrt(distance);
		return distance;
	}

	public float euclideanDistance(Point o) {
		/*
		 * if(o.getD() != this.d){ return null; }
		 */
		float distance = 0;
		for (int i = 0; i < this.p.length; i++) {
			distance += ((this.p[i] - o.getP()[i]) * (this.p[i] - o.p[i]));
		}
		distance = (float) Math.sqrt(distance);
		return distance;
	}

	/*
	 * public int getD() { return d; }
	 * 
	 * public void setD(int d) { this.d = d; }
	 */

	public float[] getP() {
		return p;
	}

	public void setP(float[] p) {
		this.p = p;
	}

	@Override
	public String toString() {
		return Arrays.toString(p).replace(",", "").replace("[", "")
				.replace("]", "");
	}

	public int getD() {
		return p.length;
	}

}
