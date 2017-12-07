package com.easou.retention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;


public class InjectRetentionAndReadDay {

	 protected static Configuration hBaseConfiguration = null;
	    protected static HTable hTable = null;
	    protected static FileWriter fileWriter = null;
	    protected static List<Put> list = null;
	    protected static int retentNum = 0;
	    protected static int readNum = 0;
	    
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
	    static void inject(String path, String colum) {
	    	
	    	BufferedReader fR = null;
	    	try {
	    		String lineTemp;
	    		fR = new BufferedReader(new FileReader(new File(path)));
	    		while ((lineTemp = fR.readLine()) != null) {
	    			String[] lineArray = lineTemp.split("\t");
	    			String key;
	    			String value;
	    			// gid 和 留存率 
	    			try {
	    				if(lineArray.length >= 6) {
	    					key = lineArray[0];
	    					value = lineArray[5];
	    					float retent = 0;
	    					retent = Float.parseFloat(value);
	    					if(retent <= 0 || retent > 1) {
	    						continue;
	    					}
	    					++ retentNum;
	    				} else {
	    					key = lineArray[0];
	    					value = lineArray[1];
	    					int read = 0;
	    					read = Integer.parseInt(value);
	    					if(read <= 0) {
	    						continue;
	    					}
	    					++ readNum;
	    				}
						addRow(hTable, key, "x", colum, value);
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
			
			if(args.length != 4) {
				System.out.println("输入参数错误:\n请依次输入:留存率结果、阅读量结果、日志文件、hbase表名");
				
				return;
			}

			String retentionPath = args[0];
			String readPath = args[1];
			String logFile = args[2];
			String tableName = args[3];
			
			System.out.println("留存率路径:" + retentionPath);
			System.out.println("阅读量路径:" + readPath);
			System.out.println("日志路径:" + logFile);
			System.out.println("hbase表名:" + tableName);
			
			//String retentionPath = "e:/day_retention.txt";
			//String readPath = "e:/day_read.txt";
			//String logFile = "e:/nject.log";
			//String tableName = "dj_test";
		
			// 获取 hbase 的 htable;
			hTable = getHtable(tableName); //-----
			writeLog("开始注入 hbase...", logFile);
			inject(retentionPath, "rt_d");
			writeLog("留存率写入数量：" + retentNum, logFile);
			inject(readPath, "rn_d");
			writeLog("阅读量写入数量：" + readNum, logFile);
			closeHbase();
			writeLog("写入完成!!!", logFile);
			
			System.out.println("ok");
		}

}
