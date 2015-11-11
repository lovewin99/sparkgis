package cn.com.testdata;

/**
 * @author wangyonglin
 *
 *         下午12:49:24
 */
public class Point {
	private int index;
	private double length;
	private double x;
	private double y;
	private String userName;
	private String floorName;
	private double coefficientOfSimilarity;
	private double sameFactor;
	private double variance;

	public double getSameFactor() {
		return sameFactor;
	}

	public void setSameFactor(double sameFactor) {
		this.sameFactor = sameFactor;
	}

	public double getVariance() {
		return variance;
	}

	public void setVariance(double variance) {
		this.variance = variance;
	}

	public double getCoefficientOfSimilarity() {
		return coefficientOfSimilarity;
	}

	public void setCoefficientOfSimilarity(double coefficientOfSimilarity) {
		this.coefficientOfSimilarity = coefficientOfSimilarity;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public double getLength() {
		return length;
	}

	public void setLength(double length) {
		this.length = length;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getFloorName() {
		return floorName;
	}

	public void setFloorName(String floorName) {
		this.floorName = floorName;
	}
}
