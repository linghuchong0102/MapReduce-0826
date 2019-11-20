package com.atguigu.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TestCompress {
	
	/**
	 * 压缩: 使用支持压缩的流将数据写出到文件中
	 */
	@Test
	public void testCompress() throws Exception {
		//获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//待压缩的文件:  d:/input/inputWord/JaneEyre.txt
		//输入流
		FSDataInputStream fis = fs.open(new Path("d:/input/inputWord/JaneEyre.txt"));
		//支持压缩的输出流
		//获取到编解码器 
		String codecClassName = "org.apache.hadoop.io.compress.DefaultCodec";
		Class<?> codecClass = Class.forName(codecClassName);
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf) ;
		//压缩后的文件
		String codecFile = "d:/codec/JaneEyre"+ codec.getDefaultExtension();
		CompressionOutputStream fos = codec.createOutputStream(fs.create(new Path(codecFile))) ;
		
		//流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		
		//关闭
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
	
	
	
	/**
	 * 解压缩: 使用支持解压缩的流将数据从压缩文件中读出
	 */
	@Test
	public void testDeCompression() throws Exception {
		// 获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 支持解压缩输入流
		// 待解压缩的文件: d:/codec/JaneEyre.deflate
		CompressionCodec codec =
			      new CompressionCodecFactory(conf).getCodec(new Path("d:/codec/JaneEyre.deflate"));
		
		CompressionInputStream fis = codec.createInputStream(fs.open(new Path("d:/codec/JaneEyre.deflate")));
		
		// 输出流
		FSDataOutputStream fos = fs.create(new Path("d:/codec/JaneEyre.txt"));
		
		// 流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		
		// 关闭
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
