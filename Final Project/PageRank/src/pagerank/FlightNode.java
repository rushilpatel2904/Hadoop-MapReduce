package pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

public class FlightNode implements Writable {

	private Text nodeId;
	private double pageRank = 0.0;
	private MapWritable neighbors;
	private int incomingLinksCount = 0;
	private boolean isNode = false;

	public FlightNode() {
		this.nodeId = new Text();
		this.neighbors = new MapWritable();
	}

	public FlightNode(String flightData) {
		this.isNode = true;
		int keyEndAt = flightData.indexOf('\t');
		if (keyEndAt != -1) {
			String key = flightData.substring(0, keyEndAt);
			this.nodeId = new Text(key);
		} else {
			this.nodeId = new Text();
		}
		flightData = flightData.substring(keyEndAt + 1);
		int nextStart = 0;
		int seperator = 0;
		String first, second;
		this.incomingLinksCount = 0;
		this.neighbors = new MapWritable();
		while (nextStart >= 0) {
			nextStart = flightData.indexOf(';');
			seperator = flightData.indexOf(',');
			first = flightData.substring(0, seperator);
			// System.out.println("First=" + first);
			if (nextStart >= 0)
				second = flightData.substring(seperator + 1, nextStart);
			else
				second = flightData.substring(seperator + 1);
			// System.out.println("Second=" + second);
			if (first.equals("PageRank"))
				this.pageRank = Double.parseDouble(second);
			else {
				int count = Integer.parseInt(second);
				this.incomingLinksCount += count;
				// this.neighbors.put(first, count);
				this.neighbors.put(new Text(first), new IntWritable(count));
			}
			flightData = flightData.substring(nextStart + 1);
		}
	}

	public FlightNode(double pageRank) {
		this.nodeId = new Text();
		this.neighbors = new MapWritable();
		this.isNode = false;
		this.pageRank = pageRank;
	}

	public void setNodeId(Text nodeId) {
		this.nodeId = nodeId;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public void setNeighbors(MapWritable neighbors) {
		this.neighbors = neighbors;
	}

	public void setIncomingLinksCount(int incomingLinksCount) {
		this.incomingLinksCount = incomingLinksCount;
	}

	public void setNode(boolean isNode) {
		this.isNode = isNode;
	}

	public Text getNodeId() {
		return nodeId;
	}

	public double getPageRank() {
		return pageRank;
	}

	public MapWritable getNeighbors() {
		return neighbors;
	}

	public int getIncomingLinksCount() {
		return incomingLinksCount;
	}

	public boolean isNode() {
		return isNode;
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append("PageRank," + this.pageRank);
		for (Writable source : this.neighbors.keySet()) {
			result.append(";" + source.toString() + ","
					+ this.neighbors.get(source).toString());
		}
		return result.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId.readFields(in);
		pageRank = in.readDouble();
		incomingLinksCount = in.readInt();
		isNode = in.readBoolean();
		neighbors.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		out.writeDouble(pageRank);
		out.writeInt(incomingLinksCount);
		out.writeBoolean(isNode);
		neighbors.write(out);
	}
}
