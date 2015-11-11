package cn.com.testdata;

/**
 * IBeacon实体类
 *
 * @author wangyonglin
 *
 *         下午1:06:37
 */
public class ScanData {
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	private String phoneNumber;
	private String phoneImei;
	private String phoneImsi;
	private String phoneModel;
	private String androidVersion;
	private String uuid;
	private String name;
	private int major;
	private long time;
	private int rssiCount;
	private double x;
	private double y;
	private String floorName;
	private String username;

	public String getFloorName() {
		return floorName;
	}

	public void setFloorName(String floorName) {
		this.floorName = floorName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
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

	public int getRssiCount() {
		return rssiCount;
	}

	public void setRssiCount(int rssiCount) {
		this.rssiCount = rssiCount;
	}

	@Override
	public String toString() {
		return "ScanData [phoneNumber=" + phoneNumber + ", phoneImei="
				+ phoneImei + ", phoneImsi=" + phoneImsi + ", phoneModel="
				+ phoneModel + ", androidVersion=" + androidVersion + ", uuid="
				+ uuid + ", name=" + name + ", major=" + major + ", minor="
				+ minor + ", txPower=" + txPower + ", rssi=" + rssi + ", mac="
				+ mac + "]";
	}

	private int minor;
	private int txPower;
	private double rssi;
	private String mac;

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public String getPhoneImei() {
		return phoneImei;
	}

	public void setPhoneImei(String phoneImei) {
		this.phoneImei = phoneImei;
	}

	public String getPhoneImsi() {
		return phoneImsi;
	}

	public void setPhoneImsi(String phoneImsi) {
		this.phoneImsi = phoneImsi;
	}

	public String getPhoneModel() {
		return phoneModel;
	}

	public void setPhoneModel(String phoneModel) {
		this.phoneModel = phoneModel;
	}

	public String getAndroidVersion() {
		return androidVersion;
	}

	public void setAndroidVersion(String androidVersion) {
		this.androidVersion = androidVersion;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getMajor() {
		return major;
	}

	public void setMajor(int major) {
		this.major = major;
	}

	public int getMinor() {
		return minor;
	}

	public void setMinor(int minor) {
		this.minor = minor;
	}

	public int getTxPower() {
		return txPower;
	}

	public void setTxPower(int txPower) {
		this.txPower = txPower;
	}

	public double getRssi() {
		return rssi;
	} 

	public void setRssi(double rssi) {
		this.rssi = rssi;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public String getMac() {
		return mac;
	}

}
