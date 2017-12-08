package com.easou.retention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.lang.Integer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *	周阅读量、周留存? 
 */

public class InjectRetentionAndReadWeek7 {

    protected static Configuration hBaseConfiguration = null;
    protected static HTable hTable = null;
    protected static FileWriter fileWriter = null;
    protected static List<Put> list = null;
    protected static int retentNum = 0;
    protected static int readNum = 0;
    protected static String logFile = null;
    
    static {        
	    hBaseConfiguration = HBaseConfiguration.create();
	    hBaseConfiguration.set("hbase.rootdir", "hdfs://10.26.22.186:9090/hbase");
	    hBaseConfiguration.set("hbase.zookeeper.quorum", "moses.datanode1,moses.datanode2,moses.datanode3,moses.datanode4,moses.namenode");
	    hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
	    list = new LinkedList<Put>();
    }
    
    static HTable getHtable(String tableName) {

    	try {
    		hTable = new HTable(hBaseConfiguration, tableName);
    	} catch (IOException e) {
    		e.printStackTrace();
    		
    		System.out.println("错误");
    		return null;
    	}

    	return hTable;
    }

    static void addRow(HTable htable, String row, String columnfamily, String column, String value) throws Exception {
        Put e = new Put(Bytes.toBytes(row));
        e.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column), Bytes.toBytes(value));
        list.add(e);
        //System.out.println("添加!!!");
    }
    
    static void commitHbase() {
    	
    	if (null == hTable) {
    		System.out.println("htable 错误!!!");
    		return;
    	}
    	try {
    		if(list.size() > 0) {
    			hTable.put(list);
    			list.clear();
    		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("commit to hbase wrong!!!");
		}
    }

    static void closeHbase() {
    	if(hTable != null) {
    		try {
				hTable.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
    /**
     *	path 尧都区的 
     * 
     */
    static void inject(String path) {
    	
    	BufferedReader fR = null;
    	try {
    		String lineTemp;
    		fR = new BufferedReader(new FileReader(new File(path)));
    		while ((lineTemp = fR.readLine()) != null) {
    			String[] lineArray = lineTemp.split("\t");
    			String key;
    			String retentTemp;
    			float retentF;
    			
    			// gid 和 留存率 
    			try {
    				
    				if(lineArray.length != 4) {
    					writeLog(lineTemp + "\twrong length", logFile);
    					
    					continue;
    				}
    				
    				key = lineArray[0];
    				retentTemp = lineArray[1];
    				
    				retentF = Float.parseFloat(retentTemp);
    				
    				if(retentF < 0 || retentF > 1) {
    					writeLog(lineTemp + "\twrong value", logFile);
    					
    					continue;
    				}
    				
    				if (retentF > 0 && retentF < 1) {
        				addRow(hTable, key, "x", "rt_w7", retentTemp);
        				
        				++ retentNum;
    				}
    				
					if(list.size() > 4096) {
						commitHbase();
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					commitHbase();
				}
    		}
    		commitHbase();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		if (fR != null) {
    			try {
					fR.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
    }


	static void writeLog(String logInfo, String logPath) {
    	
    	FileWriter fW = null;
		try {
			File fileName = new File(logPath);
			if(!fileName.exists()) {
				fileName.createNewFile();
			}
			fW = new FileWriter(fileName, true);
			fW.write(logInfo + "\n");
			fW.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		if(args.length != 3) {
			System.out.println("输入参数错误:\n请依次输入:留存率结果、日志文件、hbase表名");
			
			return;
		}

		String retentionPath = args[0];
		logFile = args[1];
		String tableName = args[2];
		
		System.out.println("留存率?:" + retentionPath);
		System.out.println("日志路径:" + logFile);
		System.out.println("hbase表名:" + tableName);
		
		// 获取 hbase? htable;
		hTable = getHtable(tableName);
		writeLog("inject hbase...", logFile);
		inject(retentionPath);
		writeLog("留存率写入数量：" + retentNum, logFile);
		writeLog("阅读量写入数量：" + readNum, logFile);
		closeHbase();
		writeLog("写入完成!!!", logFile);
		
		System.out.println("ok");
	}
}
