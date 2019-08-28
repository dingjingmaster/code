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

public class InjectRetentionAndReadWeek {

    protected static Configuration hBaseConfiguration = null;
    protected static HTable hTable = null;
    protected static FileWriter fileWriter = null;
    protected static List<Put> list = null;
    protected static int retentNum = 0;
    protected static int readNum = 0;
    protected static String logFile = null;
    
    static {        
	    hBaseConfiguration = HBaseConfiguration.create();
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
    static void injectRetent(String path) {
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
    				
    				if(lineArray.length != 8) {
    					writeLog("rt_w: " + lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				key = lineArray[0];
    				retentTemp = lineArray[5];
    				retentF = Float.parseFloat(retentTemp);
    				if (retentF > 0 && retentF <= 1) {
        				addRow(hTable, key, "x", "rt_w", retentTemp);
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
    
    static void injectReadNum(String path) {
    	
    	BufferedReader fR = null;
    	try {
    		String lineTemp;
    		fR = new BufferedReader(new FileReader(new File(path)));
    		while ((lineTemp = fR.readLine()) != null) {
    			String[] lineArray = lineTemp.split("\t");
    			String key;
    			String readTemp;
    			int readI;
    			// gid 和 留存率 
    			try {
    				
    				if(lineArray.length != 2) {
    					writeLog("rn_w: " + lineTemp + "\twrong length", logFile);
    					
    					continue;
    				}
    				key = lineArray[0];
    				readTemp = lineArray[1];
    				readI = Integer.parseInt(readTemp);
    				if (readI > 0) {
    					addRow(hTable, key, "x", "rn_w", readTemp);
    					++ readNum;
    				}
					if(list.size() > 8192) {
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
		
		if(args.length != 6) {
			System.out.println("输入参数错误:\n请依次输入:留存率结果、周阅读量结果、日志文件、hbase表名、zookeeper地址、zookeeper端口");
			return;
		}

		String retentionPath = args[0];
		String readNumPath = args[1];
		logFile = args[2];
		String tableName = args[3];
		String zookeeper = args[4];
		String zookeeperClient = args[5];
		
		System.out.println("留存率路:" + retentionPath);
		System.out.println("周阅读量:" + readNumPath);
		System.out.println("日志路径:" + logFile);
		System.out.println("hbase表名:" + tableName);
		
		//String retentionPath = "e:/day_retention.txt";
		//String readPath = "e:/day_read.txt";
		//String logFile = "e:/nject.log";
		//String tableName = "dj_test";

		// moses.namenode01,moses.datanode10,moses.datanode11,moses.datanode12,moses.datanode13
		// 2181
		hBaseConfiguration.set("hbase.zookeeper.quorum", zookeeper);
		hBaseConfiguration.set("hbase.zookeeper.property.clientPort", zookeeperClient);
	
		// 获取 hbase �? htable;
		hTable = getHtable(tableName); //-----
		writeLog("inject hbase...", logFile);
		injectRetent(retentionPath);
		injectReadNum(readNumPath);
		writeLog("留存率写入数量：" + retentNum, logFile);
		writeLog("阅读量写入数量：" + readNum, logFile);
		closeHbase();
		writeLog("写入完成!!!", logFile);
		System.out.println("ok");
	}
}
