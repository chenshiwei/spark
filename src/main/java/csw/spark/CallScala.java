package csw.spark;

public class CallScala {

	private String str;

	public String getA() {
		return str;
	}

	public void setA(String str) {
		this.str = str;
	}

	public static void main(String[] args) {
		CallJava.call();
	}
}
