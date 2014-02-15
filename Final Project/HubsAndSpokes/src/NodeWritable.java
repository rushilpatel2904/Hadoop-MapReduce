package defaul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import edu.umd.cloud9.io.array.ArrayListWritable;
public class NodeWritable implements Writable {

	Text nid;
	Text isInList;
	ArrayListWritable<NodeWritable> ins;
	ArrayListWritable<NodeWritable> outs;
	FloatWritable hubVal;
	FloatWritable spokeVal;
	LongWritable inCount;
	Map<String,NodeWritable> mapIn = new HashMap<String, NodeWritable>();
	Map<String,NodeWritable> mapOut = new HashMap<String, NodeWritable>();

	public Text getIsNode() {
		return isNode;
	}

	public void setIsNode(Text isNode) {
		this.isNode = isNode;
	}

	Text isNode;
	
	public LongWritable getInCount() {
		return inCount;
	}

	public void setInCount(LongWritable inCount) {
		this.inCount = inCount;
	}

	public Text getNid() {
		return nid;
	}

	public void setNid(Text nid) {
		this.nid = nid;
	}

	public Text getIsInList() {
		return isInList;
	}

	public void setIsInList(Text isInList) {
		this.isInList = isInList;
	}

	public ArrayListWritable<NodeWritable> getIns() {
		return ins;
	}

	public void setIns(ArrayListWritable<NodeWritable> ins) {
		this.ins = ins;
	}

	public ArrayListWritable<NodeWritable> getOuts() {
		return outs;
	}

	public void setOuts(ArrayListWritable<NodeWritable> outs) {
		this.outs = outs;
	}

	public FloatWritable getHubVal() {
		return hubVal;
	}

	public void setHubVal(FloatWritable hubVal) {
		this.hubVal = hubVal;
	}

	public FloatWritable getSpokeVal() {
		return spokeVal;
	}

	public void setSpokeVal(FloatWritable spokeVal) {
		this.spokeVal = spokeVal;
	}

	public NodeWritable() {
		nid = new Text();
		isInList = new Text();
		ins = new ArrayListWritable<NodeWritable>();
		outs = new ArrayListWritable<NodeWritable>();
		inCount = new LongWritable();
		inCount.set(0);
		hubVal = new FloatWritable();
		hubVal.set(1.0f);
		spokeVal = new FloatWritable();
		spokeVal.set(1.0f);
		isNode = new Text();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nid.readFields(in);
		hubVal.readFields(in);
		spokeVal.readFields(in);
		ins.readFields(in);
		outs.readFields(in);
		isNode.readFields(in);
		isInList.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nid.write(out);
		hubVal.write(out);
		spokeVal.write(out);
		ins.write(out);
		outs.write(out);
		isNode.write(out);
		isInList.write(out);
	}

	public NodeWritable(String nodeData) {
	
		isInList = new Text();
		inCount = new LongWritable();
		inCount.set(0);
		String[] nodeArray = nodeData.split("\t");
		nid = new Text(nodeArray[0]);
		setHubVal(new FloatWritable(Float.parseFloat(nodeArray[1])));
		setSpokeVal(new FloatWritable(Float.parseFloat(nodeArray[2])));
		ins = new ArrayListWritable<NodeWritable>();
		outs = new ArrayListWritable<NodeWritable>();
		
		isNode = new Text();
		
		String inout = nodeArray[3];
		
		String[] inoutArray = inout.split("---");
		
		String instr = null;
		String outstr = null;
		
		if (inoutArray.length ==0) return;
		if (inoutArray.length ==2) {
			if (inout.startsWith("---")) {
				outstr = inoutArray[1];
			} else {
				instr = inoutArray[0];
				outstr = inoutArray[1];
			}
		} else {
			instr = inoutArray[0];
		}
		
		
		if (instr != null && !instr.equals(null) && !instr.equals("")) {
			String[] nodes = instr.split(",");
			for (String m: nodes) {
				if (m == null || m.equals(null) || m.equals("")) continue;
				String[] values = m.split(":");
				NodeWritable mynode = new NodeWritable();
				mynode.setNid(new Text(values[0]));
				mynode.setHubVal(new FloatWritable(Float.parseFloat(values[1])));
				mynode.setSpokeVal(new FloatWritable(Float.parseFloat(values[2])));
				addInNode(mynode);
			}
		}
		
		if (outstr != null && !outstr.equals(null) && !outstr.equals("")) {
			String[] nodes = inoutArray[1].split(",");
			for (String m: nodes) {
				if (m == null || m.equals(null) || m.equals("")) continue;
				String[] values = m.split(":");
				NodeWritable mynode = new NodeWritable();
				mynode.setNid(new Text(values[0]));
				mynode.setHubVal(new FloatWritable(Float.parseFloat(values[1])));
				mynode.setSpokeVal(new FloatWritable(Float.parseFloat(values[2])));
				addOutNode(mynode);
			}
		}
	}
	
	public void addInNode(NodeWritable node) {
		
		Text nodeId = node.getNid();
		NodeWritable resultNode = null;
		if (mapIn.containsKey(nodeId.toString())) {
			resultNode = mapIn.get(nodeId.toString());
			//resultNode.inCount.set(inCount.get()+1);
			ins.clear();
			ins.addAll(mapIn.values());
			mapIn.put(nodeId.toString(), resultNode);
			ins.add(resultNode);
		} else {
			mapIn.put(nodeId.toString(), node);
//			node.inCount.set(inCount.get()+1);
			ins.add(node);
		}
		
	}
	
public void addOutNode(NodeWritable node) {
		
		Text nodeId = node.getNid();
		NodeWritable resultNode = null;
		if (mapOut.containsKey(nodeId.toString())) {
			resultNode = mapOut.get(nodeId.toString());
			outs.clear();
			outs.addAll(mapOut.values());
			mapOut.put(nodeId.toString(), resultNode);
			outs.add(resultNode);
		} else {
			mapOut.put(nodeId.toString(), node);
			outs.add(node);
		}
		
	}

	public String print() {
		StringBuffer x = new StringBuffer(nid + "\t" + hubVal + "\t" + spokeVal + "\t");
		for (NodeWritable n : (ArrayList<NodeWritable>)ins) {
			x.append(n.nid.toString() + ":" + n.hubVal + ":" + n.spokeVal + ",");
		}
		
		x.append("---");
		
		for (NodeWritable n : (ArrayList<NodeWritable>)outs) {
			x.append(n.nid.toString() + ":" + n.hubVal + ":" + n.spokeVal + ",");
		}
		
		return x.toString();
	}

	public String toString() {
		return this.print();
	}

}
