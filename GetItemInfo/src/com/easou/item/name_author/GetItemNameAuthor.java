/**
 * @author DingJing
 *
 */
package com.easou.item.name_author;


import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class GetItemNameAuthor {
	
	protected static Configuration hBaseConfiguration = null;
    protected static HTable hTable = null;
    protected static FileWriter fileWriter = null;
    protected static List<String> list = null;
    
    static {
    	hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.set("hbase.rootdir", "hdfs://10.26.22.186:9090/hbase");
        hBaseConfiguration.set("hbase.zookeeper.quorum", "moses.datanode1,moses.datanode2,moses.datanode3,moses.datanode4,moses.namenode");
        hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        
        list = new ArrayList<String>();
    }
    
    public static void write_file(FileWriter write, String line) {
    	try {
			write.write(line);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			list.add("error" + e.getMessage());
		}
    }
    
    @SuppressWarnings("deprecation")
	public static void scan(String tableName, String fileName) throws IOException {
    	FileWriter fw = null;

    	int num = 0;
    	Scan scan = null;
    	ResultScanner scanner = null;
    	
		try {
			fw = new FileWriter(fileName);
	    	hTable = new HTable(hBaseConfiguration, tableName);
	    	scan = new Scan();
	    	scan.addColumn(Bytes.toBytes("x"), Bytes.toBytes("name"));
	    	scan.addColumn(Bytes.toBytes("x"), Bytes.toBytes("norm_name"));
	    	scan.addColumn(Bytes.toBytes("x"), Bytes.toBytes("author"));
	    	scan.addColumn(Bytes.toBytes("x"), Bytes.toBytes("norm_author"));
	    	
	    	scanner = hTable.getScanner(scan);

	    	for(Result res = scanner.next(); res != null; res = scanner.next()) {
	    		String gid = "";
		    	String name = "";
		    	String normName = "";
		    	String author = "";
		    	String normAuthor = "";

	    		gid = Bytes.toString(res.getRow());
		    	for (KeyValue kv : res.list()) {
		    		if ("name".equals(Bytes.toString(kv.getQualifier()))) {
		    			name = Bytes.toString(kv.getValue());
		    		} else if ("norm_name".equals(Bytes.toString(kv.getQualifier()))) {
		    			normName = Bytes.toString(kv.getValue());
		    		} else if ("author".equals(Bytes.toString(kv.getQualifier()))) {
		    			author = Bytes.toString(kv.getValue());
		    		} else if ("norm_author".equals(Bytes.toString(kv.getQualifier()))) {
		    			normAuthor = Bytes.toString(kv.getValue());
		    		}
		    	}
		    	
	    		//System.out.print(gid + "\t" + name + "\t" + normName + "\t" + author + "\t" + normAuthor + "\n");
	    		write_file(fw, gid + "\t" + name + "\t" + normName + "\t" + author + "\t" + normAuthor + "\n");
	    		++num;
	    	}
		}catch (IOException e) {
			list.add("error:" + e.getMessage());
		} finally {
			System.out.print("拉取数据 :" + num + "条");
			fw.close();
			scanner.close();
		}
    }
	
    public static void main (String[] args) {
    	
    	if(args.length != 1) {
    		System.out.println("please input tablename and log file");
    	}

    	String tableName = args[0];
    	String fileName = args[1];
    	//String tableName = "item_info";
    	//String fileName = "./item_name_author.txt";
    	
    	try {
			scan(tableName, fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error");
		}
    	for(String i : list) {
    		System.out.println("error: " + i);
    	}
    	
    	return;
    }
}
