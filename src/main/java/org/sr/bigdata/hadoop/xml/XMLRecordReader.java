package org.sr.bigdata.hadoop.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @Organ: Inspur Group
 * @Teams: Big Data Team
 * @Author: zhouzhd {2014年6月11日 上午11:16:36}
 * @Mail: zzd338@163.com
 * 
 * @ClassName: XMLRecordReader
 * @Description:
 * 
 * 
 */
@InterfaceAudience.LimitedPrivate({ "MapReduce", "Pig" })
@InterfaceStability.Evolving
public class XMLRecordReader extends RecordReader<LongWritable, InputStream> {

	private static final Log LOG = LogFactory.getLog(XMLRecordReader.class);
	private static String LOG_INFO_HEAD = ">>>>>>>>>> ";

	private boolean next = true; // if has next value
	private LongWritable key = new LongWritable(1); // random key
	private InputStream value = null; // xml split file
	private XmlSplit xmlSplit = null; // XmlSplit object
	private SplitPos splitPos = SplitPos.HEAD; // SplitPos object
	private static final int each = 5000000; // default size for read from xml
												// file each time

	public XMLRecordReader() {
	}

	public boolean nextKeyValue() throws IOException {

		return next;
	}

	@Override
	public LongWritable getCurrentKey() {
		LOG.info(LOG_INFO_HEAD + "getCurrentKey");
		return key;
	}

	@Override
	public InputStream getCurrentValue() {

		LOG.info(LOG_INFO_HEAD + "getCurrentValue");
		next = false;
		return value;
	}

	public float getProgress() throws IOException {

		LOG.info(LOG_INFO_HEAD + "method getProgress");
		if (next) {
			return 0.0f;
		} else {
			return 1.0f;
		}
	}

	@Override
	public void close() throws IOException {

		if (value != null) {
			value.close();
			LOG.info(LOG_INFO_HEAD + "value input stream closed");
		}
	}

	/**
	 * @Author: zhouzhd {2014年6月11日 上午11:29:59}
	 * @Version：
	 * @Status: accomplish
	 * @Title: initialize
	 * @Description:
	 * 
	 * @param split
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		xmlSplit = (XmlSplit) split;
		Configuration job = context.getConfiguration();

		final FileSystem fs = xmlSplit.getPath().getFileSystem(job);
		long fileLen = fs.getFileStatus(xmlSplit.getPath()).getLen();
		long length = xmlSplit.getLength();
		long start = xmlSplit.getStart();
		int index = 0;

		LOG.info(LOG_INFO_HEAD + "file 	  path: " + xmlSplit.getPath());
		LOG.info(LOG_INFO_HEAD + "file  length: " + fileLen);
		LOG.info(LOG_INFO_HEAD + "split  start: " + start);
		LOG.info(LOG_INFO_HEAD + "split length: " + length);
		LOG.info(LOG_INFO_HEAD + "split   head: " + job.get(XmlConf.HEAD));
		LOG.info(LOG_INFO_HEAD + "split   tail: " + job.get(XmlConf.TAIL));

		FSDataInputStream fsdis = fs.open(xmlSplit.getPath());
		fsdis.skip(start);

		byte[] head = job.get(XmlConf.HEAD).getBytes();
		byte[] tail = job.get(XmlConf.TAIL).getBytes();
		byte[] splitSeg = new byte[getWholeLength(start, length, fileLen, head.length, tail.length)];

		// handle head
		if (splitPos.compareTo(SplitPos.TAIL) == 0 || splitPos.compareTo(SplitPos.BODY) == 0) {
			for (int i = 0; i < head.length; i++) {
				splitSeg[i] = head[i];
			}
			index += head.length;
		}

		// handle body
		index = fillBody(fsdis, index, length, splitSeg);

		// handle tail
		if (splitPos.compareTo(SplitPos.HEAD) == 0 || splitPos.compareTo(SplitPos.BODY) == 0) {
			for (int i = 0; i < tail.length; i++) {
				splitSeg[index + i] = tail[i];
			}
		}

		fsdis.close();
		fs.close();

		value = new ByteArrayInputStream(splitSeg);
		LOG.info(LOG_INFO_HEAD + "initialize finished, value size: " + splitSeg.length);
	}

	/**
	 * @Author: zhouzhd {Jun 10, 2014 3:06:52 PM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: fillBody
	 * @Description:
	 * 
	 * @param ins
	 * @param index
	 * @param length
	 * @param splitSeg
	 * @return
	 */
	public int fillBody(InputStream ins, int index, long length, byte[] splitSeg) {

		int readed = 0, real = 0;
		while (true) {
			try {
				int available = ins.available(); // get available size
				if (available == 0) { // reach file tail
					break;
				} else {
					int loc = each < available ? each : available; // read size
																	// for this
																	// time
					if (readed + loc < length) {
						real = ins.read(splitSeg, index, loc);
					} else {
						int last = new Long(length).intValue() - readed;
						real = ins.read(splitSeg, index, last);
					}
					index += real;
					readed += real;
					if (readed == length) {// when get all bytes, break
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return index;
	}

	/**
	 * @Author: zhouzhd {Jun 10, 2014 12:24:02 PM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: getWholeLength
	 * @Description:
	 * 
	 * @param start
	 * @param length
	 * @param fileLen
	 * @param head
	 * @param tail
	 * @return
	 */
	public int getWholeLength(long start, long length, long fileLen, int head, int tail) {

		if (start > 0) {
			if (start + length < fileLen) {// file body
				splitPos = SplitPos.BODY;
				return head + new Long(length).intValue() + tail;
			} else {// file tail
				splitPos = SplitPos.TAIL;
				return head + new Long(length).intValue();
			}
		} else {
			if (start + length < fileLen) {// file head
				splitPos = SplitPos.HEAD;
				return new Long(length).intValue() + tail;
			} else {// whole file
				splitPos = SplitPos.WHOLE;
				return new Long(length).intValue();
			}
		}
	}

	public enum SplitPos {
		HEAD, BODY, TAIL, WHOLE;
	}
}