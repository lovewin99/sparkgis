package cn.com.testdata;

public class SensorData {
	private float x;
	private float y;
	private float z;
	private long time;

	@Override
	public String toString() {
		return "SensorData [x=" + x + ", y=" + y + ", z=" + z + ", time="
				+ time + "]";
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public float getX() {
		return x;
	}

	public void setX(float x) {
		this.x = x;
	}

	public float getY() {
		return y;
	}

	public void setY(float y) {
		this.y = y;
	}

	public float getZ() {
		return z;
	}

	public void setZ(float z) {
		this.z = z;
	}

}
