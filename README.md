HadoopXMLTool
=============

利用MapReduce对大XML文件进行解析处理, 实例看DemoMain.java

###1、	参数说明
	
	1) 	mapreduce.input.xmlInputFormat.head
		Xml填充文件头部信息，用于对中间位置和最后位置的分块添加文件头
		
		举例：
	  	conf.set("mapreduce.input.xmlInputFormat.head", "<html><body>");
	  	
	2) 	mapreduce.input.xmlInputFormat.tail
		Xml填充文件尾部信息，用于对中间位置和开头位置的分块添加文件尾
		
		举例：
	  	conf.set("mapreduce.input.xmlInputFormat.tail", "</body></html>");
	  	
	3) 	mapreduce.input.xmlInputFormat.tailTag
		分割结束标签，多个标签使用逗号分隔
		
		举例：
	  	conf.set("mapreduce.input.xmlInputFormat.tailTag", "</tr>,</th>");
	
	
###2、	开发说明
	
		Job的InputFormat设置为XmlInputFormat.class
		
		举例：
 	 	job.setInputFormatClass(XmlInputFormat.class);
 	 	
 	 	
###3、	程序说明
		1) org.sr.hadoop.xml.XmlConf 配置类，含有配置参数信息
		2) org.sr.hadoop.xml.XmlInputFormat	用于生成xml split信息
		3) org.sr.hadoop.xml.XmlRecordReader 用于构造xml分块数据
		4) org.sr.hadoop.xml.XmlSplit xml split分块对象类
		
		5) org.sr.hadoop.xml.DemoMain 测试MapReduce
		
		
###4、	程序执行
		hadoop jar /home/DemoMain.jar /Test.xml /log/rc 9000000 10000000
		
		split说明:
		
		file size: 166423858
		blockSize: 67108864 [minSize:9000000 maxSize:10000000]
		splitSize: 10000000
		Total # of splits: 17
