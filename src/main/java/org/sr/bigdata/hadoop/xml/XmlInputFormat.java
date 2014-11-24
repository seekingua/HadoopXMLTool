package org.sr.bigdata.hadoop.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @Organ: Inspur Group
 * @Teams: Big Data Team
 * @Author: zhouzhd {2014年6月10日 下午8:47:15}
 * @Mail: zzd338@163.com
 * 
 * @ClassName: XmlInputFormat
 * @Description:
 * 
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class XmlInputFormat extends FileInputFormat<LongWritable, InputStream> {

	private static final Log LOG = LogFactory.getLog(XmlInputFormat.class);
	private static String LOG_INFO_HEAD = ">>>>>>>>>> ";

	private static final double SPLIT_SLOP = 1.1; // 10% slop

	private int tagMaxLength = 0; // max length in all tags
	private int tagMatchLength = 0; // length for match tag

	public XmlInputFormat() {
	}

	/**
	 * @Author: zhouzhd {2014年6月11日 上午11:31:21}
	 * @Version：
	 * @Status: accomplish
	 * @Title: getSplits
	 * @Description:
	 * 
	 * @param job
	 * @return
	 * @throws IOException
	 */
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);

		// set max tag length for buffer reset
		setTagMaxLength(job);

		for (FileStatus file : files) {
			Path path = file.getPath();
			long length = file.getLen();

			LOG.info(LOG_INFO_HEAD + "file path: " + path);
			LOG.info(LOG_INFO_HEAD + "file size: " + length);

			if (length != 0) {
				FileSystem fs = path.getFileSystem(job.getConfiguration());
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);

				if (isSplitable(job, path)) {
					long blockSize = file.getBlockSize();
					long splitSize = computeSplitSize(blockSize, minSize, maxSize);

					LOG.info(LOG_INFO_HEAD + "blockSize: " + blockSize + " [minSize:" + minSize + " maxSize:" + maxSize + "]");
					LOG.info(LOG_INFO_HEAD + "splitSize: " + splitSize);

					long bytesRemaining = length;
					long tgOffSet = 0;
					while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
						LOG.info(LOG_INFO_HEAD + "bytesRemaining / splitSize: " + bytesRemaining + "/" + splitSize + "[" + ((double) bytesRemaining) / splitSize + "]");
						int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);

						tgOffSet = verifySplitSize(path, fs, length - bytesRemaining + splitSize, job);
						LOG.info(LOG_INFO_HEAD + "[split pos: " + (bytesRemaining == length ? "HEAD]" : "BODY]") + " [start: " + (length - bytesRemaining) + "] [length: " + (splitSize + tgOffSet + tagMatchLength) + "]");

						splits.add(makeXmlSplit(path, length - bytesRemaining, splitSize + tgOffSet + tagMatchLength, blkLocations[blkIndex].getHosts()));
						bytesRemaining -= splitSize + tgOffSet + tagMatchLength;
					}
					if (bytesRemaining != 0) {
						LOG.info(LOG_INFO_HEAD + "[last  pos: " + (bytesRemaining == length ? "WHOLE]" : "TAIL]" + " [start: " + (length - bytesRemaining) + "] [length: " + bytesRemaining + "]"));
						int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);

						splits.add(makeXmlSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkIndex].getHosts()));
					}
				} else { // not splitable
					LOG.info(LOG_INFO_HEAD + "[not splitable: WHOLE]" + " [start: 0] [length: " + length + "]");
					splits.add(makeXmlSplit(path, 0, length, blkLocations[0].getHosts()));
				}
			} else {
				// Create empty hosts array for zero length files
				LOG.info(LOG_INFO_HEAD + "[zero length files: WHOLE]" + " [start: 0] [length: 0]");
				splits.add(makeXmlSplit(path, 0, 0, new String[0]));
			}
		}

		// Save the number of input files for metrics/loadgen
		LOG.info("Total # of splits: " + splits.size());
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

		return splits;
	}

	/**
	 * @Author: zhouzhd {Jun 6, 2014 10:03:24 AM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: makeXmlSplit
	 * @Description: get XmlSplit instance
	 * 
	 * @param file
	 * @param start
	 * @param length
	 * @param hosts
	 * @return
	 */
	protected XmlSplit makeXmlSplit(Path file, long start, long length, String[] hosts) {

		return new XmlSplit(file, start, length, hosts);
	}

	/**
	 * @Author: zhouzhd {Jun 6, 2014 9:34:18 AM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: verifySplitSize
	 * @Description: verify the block size and return the right offset
	 * 
	 * @param path
	 * @param hdfs
	 * @param skipSize
	 * @param job
	 * @return
	 */
	protected long verifySplitSize(Path path, FileSystem hdfs, long skipSize, JobContext job) {

		LOG.info(LOG_INFO_HEAD + "start verify the block offset");

		InputStream in = null;
		long tgOffSet = 0;
		try {
			in = hdfs.open(path);
			in.skip(skipSize);
			int readLen = 90, bufLen = readLen + tagMaxLength; // read size for
																// each time,
																// buffer size
			byte[] buf = new byte[bufLen];
			confirmRead(buf, 0, bufLen, in);
			do {
				String info = new String(buf);
				int index = getIndex(info, job);
				LOG.info(LOG_INFO_HEAD + "[offSet value: " + tgOffSet + "] [tagMaxLength value: " + tagMaxLength + "] [index value: " + index + "] [tagMatchLength value: " + tagMatchLength + "]");
				if (index >= 0) {
					tgOffSet += index;
					break;
				} else {
					System.arraycopy(buf, readLen, buf, 0, tagMaxLength);
					confirmRead(buf, tagMaxLength, readLen, in);
					tgOffSet += readLen;
				}
			} while (true);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}

		LOG.info(LOG_INFO_HEAD + "the block offset is: " + tgOffSet);
		return tgOffSet;
	}

	/**
	 * @Author: zhouzhd {2014年11月4日 上午9:25:14}
	 * @Version：
	 * @Status: accomplish
	 * @Title: confirmRead
	 * @Description:
	 * 
	 * @param buf
	 * @param offSet
	 * @param length
	 * @param in
	 * @throws IOException
	 */
	public void confirmRead(byte[] buf, int offSet, int length, InputStream in) throws IOException {

		int readed = 0;
		while (readed < length) {
			readed += in.read(buf, offSet + readed, length - readed);
		}
	}

	/**
	 * @Author: zhouzhd {Jun 6, 2014 10:10:46 AM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: setTagMaxLength
	 * @Description: get the max tag length from
	 *               "mapreduce.input.xmlInputFormat.tailTag" and set to the
	 *               parameter tagMaxLength
	 * 
	 * @param job
	 */
	private void setTagMaxLength(JobContext job) {

		String tailTag = job.getConfiguration().get(XmlConf.TAIL_TAG);
		for (String tag : tailTag.split(",")) {
			tagMaxLength = tagMaxLength > tag.length() ? tagMaxLength : tag.length();
		}
		LOG.info(LOG_INFO_HEAD + "tagMaxLength:" + tagMaxLength);
	}

	/**
	 * @Author: zhouzhd {Jun 6, 2014 9:32:39 AM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: getIndex
	 * @Description: get the first match tag from buf and return the value of
	 *               indexof
	 * 
	 * @param info
	 * @param job
	 */
	private int getIndex(String info, JobContext job) {

		String tailTag = job.getConfiguration().get(XmlConf.TAIL_TAG);
		for (String tag : tailTag.split(",")) {
			if (info.contains(tag)) {
				tagMatchLength = tag.length();
				return info.indexOf(tag);
			}
		}
		return -1;
	}

	/**
	 * @Author: zhouzhd {Jun 6, 2014 10:44:20 AM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: createRecordReader
	 * @Description:
	 * 
	 * @param split
	 * @param context
	 * @return
	 */
	@Override
	public RecordReader<LongWritable, InputStream> createRecordReader(InputSplit split, TaskAttemptContext context) {

		return new XMLRecordReader();
	}

	/**
	 * @Author: zhouzhd {Jun 6, 2014 10:53:32 AM}
	 * @Version：
	 * @Status: accomplish
	 * @Title: isSplitable
	 * @Description: mark the file is splitable
	 * 
	 * @param context
	 * @param filename
	 * @return
	 */
	@Override
	public boolean isSplitable(JobContext context, Path filename) {
		return true;
	}
}
