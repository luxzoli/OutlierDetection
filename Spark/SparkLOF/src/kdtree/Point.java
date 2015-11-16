package kdtree;

import java.io.Serializable;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Point implements Serializable {
	
	private int d;
	private float[] p;
	private boolean isCorePoint;
	private float kDistance;
	private boolean isReady;
	private Point[] nearestNeighbors = null;
	private float[] neighborLRDs = null;
	private float LRD;
	private float LOFScore;
	private long ID;
	private int cellID;

	public Point(float[] p) {
		this.d = p.length;
		this.p = p;
	}

	// TODO: nem teljes
	public Point(Point p) {
		this.d = p.d;
		this.p = new float[p.p.length];
		for (int i = 0; i < p.p.length; i++) {
			this.p[i] = p.p[i];
		}
		this.isReady = p.isReady;
		this.isCorePoint = p.isCorePoint;
		this.kDistance = p.kDistance;
		this.LRD = p.LRD;
		this.LOFScore = p.LOFScore;
	}

	private Point() {

	}

	public Point(String p) {
		if (p.endsWith("r")) {
			p = p.substring(0, p.length() - 2);
			StringTokenizer st = new StringTokenizer(p, " ");
			this.ID = Long.parseLong(st.nextToken(" "));
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = true;
		} else {
			p = p.substring(0, p.length() - 2);
			StringTokenizer stu = new StringTokenizer(p, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), " ");
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			nearestNeighbors = new Point[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, " ");
				float[] coords = new float[d];
				for (int j = 0; j < d; j++) {
					String s = sto.nextToken();
					coords[j] = Float.parseFloat(s);
				}
				Point op = new Point();
				op.p = coords;
				op.d = d;
				nearestNeighbors[i] = op;
			}
		}

	}

	public Point(String p, String delim, boolean mark) {
		StringTokenizer st = new StringTokenizer(p, delim);
		this.ID = Long.parseLong(st.nextToken(delim));
		int d = st.countTokens();
		this.d = d;
		this.p = new float[d];
		for (int i = 0; i < d; i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
	}

	public Point(String p, boolean mark) {
		StringTokenizer st = new StringTokenizer(p, " ");
		this.ID = Long.parseLong(st.nextToken());
		int d = st.countTokens();
		this.d = d;
		this.p = new float[d];
		for (int i = 0; i < d; i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
	}

	public Point(String p, String delim) {
		if (p.endsWith("r")) {
			p = p.substring(0, p.length() - 2);
			StringTokenizer st = new StringTokenizer(p, delim);
			this.ID = Long.parseLong(st.nextToken(delim));
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = true;
		} else {
			p = p.substring(0, p.length() - 3);
			StringTokenizer stu = new StringTokenizer(p, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), delim);
			this.ID = Long.parseLong(st.nextToken(delim));
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			nearestNeighbors = new Point[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, delim);
				float[] coords = new float[d];
				for (int j = 0; j < d; j++) {
					String s = sto.nextToken();
					coords[j] = Float.parseFloat(s);
				}
				Point op = new Point();
				op.p = coords;
				op.d = d;
				nearestNeighbors[i] = op;
			}
		}
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
		String point = ID + " " + Arrays.toString(p).replace(",", "").replace("[", "").replace("]", "") + " "
				+ kDistance;
		String neighbors = "|";
		if (!isReady) {
			for (Point neighbor : nearestNeighbors) {
				neighbors += Arrays.toString(neighbor.p).replace(",", "").replace("[", "").replace("]", "") + ";";
			}
			return point + neighbors + " n";
		} else {
			return point + " r";
		}
	}

	public String toSimpleString() {
		return ID + " " + Arrays.toString(p).replace(",", "").replace("[", "").replace("]", "");
	}

	public String toCoordsString() {
		return Arrays.toString(p).replace(",", "").replace("[", "").replace("]", "");
	}

	public String toLRDString() {
		String point = ID + " " + Arrays.toString(p).replace(",", "").replace("[", "").replace("]", "") + " "
				+ kDistance + " " + LRD;
		String neighbors = "|";
		if (isReady) {
			return point + " r";
		} else {
			for (Point neighbor : nearestNeighbors) {
				neighbors += Arrays.toString(neighbor.p).replace(",", "").replace("[", "").replace("]", "") + " "
						+ neighbor.kDistance + ";";
			}
			return point + neighbors + " n";
		}
	}

	public String toLOFString() {
		String point = ID + " " + Arrays.toString(p).replace(",", "").replace("[", "").replace("]", "") + " "
				+ kDistance + " " + LRD;
		String neighbors = "|";
		// if(nearestNeighbors != null){
		if (isReady) {
			return point + " " + LOFScore + " r";
		} else {
			for (Point neighbor : nearestNeighbors) {
				neighbors += Arrays.toString(neighbor.p).replace(",", "").replace("[", "").replace("]", "") + " "
						+ neighbor.LRD + ";";
			}
			return point + neighbors + " " + LOFScore + " n";
		}
	}

	public static Point fromLRDString(String LRDPoint, String delim) {
		Point p = new Point();
		if (LRDPoint.endsWith("r")) {
			LRDPoint = LRDPoint.substring(0, LRDPoint.length() - 2);
			StringTokenizer st = new StringTokenizer(LRDPoint, delim);
			p.ID = Long.parseLong(st.nextToken());
			int d = st.countTokens() - 2;
			p.d = d;
			p.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				p.p[i] = Float.parseFloat(s);
			}
			p.kDistance = Float.parseFloat(st.nextToken());
			p.LRD = Float.parseFloat(st.nextToken());
			p.isReady = true;
		} else {
			LRDPoint = LRDPoint.substring(0, LRDPoint.length() - 3);
			StringTokenizer stu = new StringTokenizer(LRDPoint, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), delim);
			p.ID = Long.parseLong(st.nextToken(delim));
			int d = st.countTokens() - 2;
			p.d = d;
			p.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				p.p[i] = Float.parseFloat(s);
			}
			p.kDistance = Float.parseFloat(st.nextToken());
			p.LRD = Float.parseFloat(st.nextToken());
			p.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			p.nearestNeighbors = new Point[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, delim);
				float[] coords = new float[d];
				for (int j = 0; j < d; j++) {
					String s = sto.nextToken();
					coords[j] = Float.parseFloat(s);
				}
				Point op = new Point();
				op.p = coords;
				op.d = d;
				op.kDistance = Float.parseFloat(sto.nextToken());
				p.nearestNeighbors[i] = op;
			}
		}
		return p;
	}

	// TODO
	public static Point fromLOFString(String LOFPoint, String delim) {
		Point p = new Point();
		if (LOFPoint.endsWith("r")) {
			LOFPoint = LOFPoint.substring(0, LOFPoint.length() - 2);
			p.LOFScore = Float.parseFloat(LOFPoint.substring(LOFPoint.lastIndexOf(" ") + 1));
			LOFPoint = LOFPoint.substring(0, LOFPoint.lastIndexOf(" "));
			StringTokenizer st = new StringTokenizer(LOFPoint, delim);
			p.ID = Long.parseLong(st.nextToken());
			int d = st.countTokens() - 2;
			p.d = d;
			p.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				p.p[i] = Float.parseFloat(s);
			}
			p.kDistance = Float.parseFloat(st.nextToken());
			p.LRD = Float.parseFloat(st.nextToken());
			p.isReady = true;
		} else {
			LOFPoint = LOFPoint.substring(0, LOFPoint.length() - 3);
			p.LOFScore = Float.parseFloat(LOFPoint.substring(LOFPoint.lastIndexOf(" ") + 1));
			LOFPoint = LOFPoint.substring(0, LOFPoint.lastIndexOf(" "));
			StringTokenizer stu = new StringTokenizer(LOFPoint, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), delim);
			p.ID = Long.parseLong(st.nextToken(delim));
			int d = st.countTokens() - 2;
			p.d = d;
			p.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				p.p[i] = Float.parseFloat(s);
			}
			p.kDistance = Float.parseFloat(st.nextToken());
			p.LRD = Float.parseFloat(st.nextToken());
			p.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			p.nearestNeighbors = new Point[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, delim);
				float[] coords = new float[d];
				for (int j = 0; j < d; j++) {
					String s = sto.nextToken();
					coords[j] = Float.parseFloat(s);
				}
				Point op = new Point();
				op.p = coords;
				op.d = d;
				op.LRD = Float.parseFloat(sto.nextToken());
				p.nearestNeighbors[i] = op;
			}
		}
		return p;
	}

	public boolean isCorePoint() {
		return isCorePoint;
	}

	public void setCorePoint(boolean isCorePoint) {
		this.isCorePoint = isCorePoint;
	}

	public float getkDistance() {
		return kDistance;
	}

	public void setkDistance(float kDistance) {
		this.kDistance = kDistance;
	}

	public boolean isReady() {
		return isReady;
	}

	public void setReady(boolean isReady) {
		this.isReady = isReady;
	}

	public Point[] getNearestNeighbors() {
		return nearestNeighbors;
	}

	public void setNearestNeighbors(Point[] nearestNeighbors) {
		this.nearestNeighbors = new Point[nearestNeighbors.length];
		for (int i = 0; i < nearestNeighbors.length; i++) {
			this.nearestNeighbors[i] = new Point(nearestNeighbors[i]);
		}
	}

	public float getLocalReachabilityDensity() {
		return LRD;
	}

	public void setLocalReachabilityDensity(float localReachabilityDensity) {
		this.LRD = localReachabilityDensity;
	}

	public static float calculateLRD(Point p) {
		float LRD = 0;
		float card = (float) p.nearestNeighbors.length;
		for (Point o : p.nearestNeighbors) {
			float distance = Point.euclideanDistance(o, p);
			LRD += ((o.kDistance > distance ? o.kDistance : distance));
		}
		LRD = card / LRD;
		return LRD;
	}

	public float calculateLRD() {
		float LRD = 0;
		float card = (float) nearestNeighbors.length;
		for (Point o : nearestNeighbors) {
			float distance = Point.euclideanDistance(o, this);
			LRD += ((o.kDistance > distance ? o.kDistance : distance));
		}
		LRD = card / LRD;
		this.LRD = LRD;
		return LRD;
	}

	public void calculateLOFScore() {
		LOFScore = 0;
		float card = (float) nearestNeighbors.length;
		for (int j = 0; j < this.nearestNeighbors.length; j++) {
			Point o = nearestNeighbors[j];
			LOFScore += o.LRD;
		}
		LOFScore /= (LRD * card);
	}

	public static float calculateLOFScore(Point p) {
		float LOFScore = 0;
		float card = (float) p.nearestNeighbors.length;
		for (int j = 0; j < p.nearestNeighbors.length; j++) {
			Point o = p.nearestNeighbors[j];
			LOFScore += (o.LRD);
		}
		LOFScore /= (p.LRD * card);
		return LOFScore;
	}

	public int getCellID() {
		return cellID;
	}

	public void setCellID(int cellID) {
		this.cellID = cellID;
	}

	public long getID() {
		return ID;
	}

	public void setID(long ID) {
		this.ID = ID;
	}

	public float[] getNeighborLRDs() {
		return neighborLRDs;
	}

	public void setNeighborLRDs(float[] neighborLRDs) {
		this.neighborLRDs = neighborLRDs;
	}

	public float getLOFScore() {
		return LOFScore;
	}

	public void setLOFScore(float lOFScore) {
		LOFScore = lOFScore;
	}

}
