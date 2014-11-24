package org.sr.bigdata.hadoop.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

/**
 * @Organ: Inspur Group
 * @Teams: Big Data Team
 * @Author: zhouzhd {2014年6月10日 下午8:46:49}
 * @Mail: zzd338@163.com
 * 
 * @ClassName: DemoMain
 * @Description:
 * 
 * 
 */
public class DemoMain {

	public static class Xml_Mapper extends Mapper<Object, InputStream, IntWritable, IntWritable> {

		private static final Log LOG = LogFactory.getLog(Xml_Mapper.class);

		@SuppressWarnings("unchecked")
		public void map(Object key, InputStream ins, Context context) {

			try {
				LOG.info(">>>>>>>>>>>>>>>>>>>> map start");

				SAXBuilder builder = new SAXBuilder();
				Document doc = builder.build(ins);
				Element root = doc.getRootElement();
				List<Element> first = root.getChild("rnc").getChild("class").getChild("measurement").getChildren();
				for (Element el : first) {
					LOG.info(">>>>>>>>>>>>>>>>>>>> v size: " + el.getChildren("v").size());
					context.write(new IntWritable(el.getChildren("v").size()), new IntWritable(1));
				}

				LOG.info(">>>>>>>>>>>>>>>>>>>> map end");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class Xml_Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4) {
			System.exit(2);
		}
		if (!otherArgs[2].equals("0")) {
			conf.set("mapreduce.input.fileinputformat.split.minsize", otherArgs[2]);
		}
		if (!otherArgs[3].equals("0")) {
			conf.set("mapreduce.input.fileinputformat.split.maxsize", otherArgs[3]);
		}
		conf.set("mapreduce.input.xmlInputFormat.head", "<bulkPmMrDataFile><rnc><class><measurement>");
		conf.set("mapreduce.input.xmlInputFormat.tail", "</measurement></class></rnc></bulkPmMrDataFile>");
		conf.set("mapreduce.input.xmlInputFormat.tailTag", "</object>,</smr>");

		Job job = Job.getInstance(conf, "xml");

		// input format
		job.setInputFormatClass(XmlInputFormat.class);
		job.setJarByClass(DemoMain.class);

		job.setMapperClass(Xml_Mapper.class);
		job.setReducerClass(Xml_Reducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		XmlInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
