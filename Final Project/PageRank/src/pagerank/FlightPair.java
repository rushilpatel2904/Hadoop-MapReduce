package pagerank;

import java.io.*;
import org.apache.hadoop.io.*;

public class FlightPair implements Writable {

	private Text airport;
	private int count;

	public FlightPair() {
		this.airport = new Text();
	}
	
	public FlightPair(Text airport, int count) {
		this.airport = airport;
		this.count = count;
	}

//	public void set(Text airport, int count) {
//		this.airport = airport;
//		this.count = count;
//	}

	public Text getAirport() {
		return airport;
	}

	public int getCount() {
		return count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		airport.write(out);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		airport.readFields(in);
		count = in.readInt();
	}

	// @Override
	// public int hashCode() {
	// return airport * 163 + count;
	// }

	// @Override
	// public boolean equals(Object o) {
	// if (o instanceof FlightPair) {
	// FlightPair ip = (FlightPair) o;
	// return airport == ip.airport && count == ip.count;
	// }
	// return false;
	// }

	@Override
	public String toString() {
		return airport + "\t" + count;
	}

	// @Override
	// public int compareTo(FlightPair ip) {
	// int cmp = compare(airport, ip.airport);
	// if (cmp != 0) {
	// return cmp;
	// }
	// return compare(count, ip.count);
	// }
	//
	// /**
	// * Convenience method for comparing two int.
	// */
	// public static int compare(int a, int b) {
	// return (a < b ? -1 : (a == b ? 0 : 1));
	// }

}
