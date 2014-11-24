package org.sr.bigdata.hadoop.xml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @Organ: Inspur Group
 * @Teams: Big Data Team
 * @Author: zhouzhd {2014年6月11日 上午11:05:35}
 * @Mail: zzd338@163.com
 * 
 * @ClassName: XmlSplit
 * @Description:
 * 
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class XmlSplit extends InputSplit implements Writable {

	private Path file; // xml file path
	private long start; // xml split start pos
	private long length; // xml split length
	private String[] hosts; // the hosts where the xml file locate

	public XmlSplit() {
	}

	/**
	 * @Author: zhouzhd {2014年6月11日 上午11:13:53}
	 * @Version：
	 * @Status: accomplish
	 * @Description: constructs a split
	 * 
	 * @param file
	 * @param start
	 * @param length
	 * @param hosts
	 */
	public XmlSplit(Path file, long start, long length, String[] hosts) {

		this.file = file;
		this.start = start;
		this.length = length;
		this.hosts = hosts;
	}

	public Path getPath() {
		return file;
	}

	public long getStart() {
		return start;
	}

	public long getLength() {
		return length;
	}

	public String toString() {

		return file + ":" + start + "+" + length;
	}

	public String[] getLocations() throws IOException {

		if (this.hosts == null) {
			return new String[] {};
		} else {
			return this.hosts;
		}
	}

	public void write(DataOutput out) throws IOException {

		Text.writeString(out, file.toString());
		out.writeLong(start);
		out.writeLong(length);
	}

	public void readFields(DataInput in) throws IOException {

		file = new Path(Text.readString(in));
		start = in.readLong();
		length = in.readLong();
		hosts = null;
	}
}
