package point;

public class Rectangle {
	private int d;
	private double[] lowerBoundary;
	private double[] upperBoundary;
	
	//this is a mess
	public Rectangle(Point[] points, double[] parentLowerBoundary, double[] parentUpperBoundary){
		this.d = points[0].getP().length;
		lowerBoundary = new double[d];
		upperBoundary = new double[d];
	}
	
	public boolean isInside(Point point, double epsilon){
		return true;
	}
}
