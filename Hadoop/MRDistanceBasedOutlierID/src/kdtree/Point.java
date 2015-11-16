package kdtree;

import java.util.Arrays;
import java.util.StringTokenizer;

public class Point {
	private int d;
	private float[] p;
	private boolean isCorePoint;
	private long ID;

	public Point(float[] p) {
		this.d = p.length;
		this.p = p;
	}

	public Point(String p) {
		StringTokenizer st = new StringTokenizer(p, " ");
		this.ID = Long.parseLong(st.nextToken(" "));
		int d = st.countTokens() - 1;
		this.d = d;
		this.p = new float[d];
		for (int i = 0; i < d; i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
		String isCoreString = st.nextToken();
		if (isCoreString.equals("core")) {
			isCorePoint = true;
		} else {
			isCorePoint = false;
		}
	}

	public Point(String p, String delim) {
		StringTokenizer st = new StringTokenizer(p, delim);
		this.ID = Long.parseLong(st.nextToken(delim));
		int d = st.countTokens() - 1;
		this.d = d;
		this.p = new float[d];
		for (int i = 0; i < d; i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
		String isCoreString = st.nextToken();
		if (isCoreString.equals("core")) {
			isCorePoint = true;
		} else {
			isCorePoint = false;
		}
	}

	public static float euclideanDistance(Point p1, Point p2) {
		/*
		 * if(o.getD() != this.d){ return null; }
		 */
		float distance = 0;
		for (int i = 0; i < p1.d; i++) {
			distance += ((p1.p[i] - p2.p[i]) * (p1.p[i] - p2.p[i]));
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
		for (int i = 0; i < this.d; i++) {
			distance += ((this.p[i] - o.getP()[i]) * (this.p[i] - o.p[i]));
		}
		distance = (float) Math.sqrt(distance);
		return distance;
	}

	public int getD() {
		return p.length;
	}

	public float[] getP() {
		return p;
	}

	public void setP(float[] p) {
		this.p = p;
	}

	@Override
	public String toString() {
		return ID + " " + Arrays.toString(p).replace(",", "").replace("[", "")
				.replace("]", "");
	}

	public boolean isCorePoint() {
		return isCorePoint;
	}

	public void setCorePoint(boolean isCorePoint) {
		this.isCorePoint = isCorePoint;
	}

	public long getID() {
		return ID;
	}

	public void setID(long iD) {
		ID = iD;
	}

}
