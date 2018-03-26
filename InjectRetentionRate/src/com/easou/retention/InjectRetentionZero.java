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


public class InjectRetentionZero {

	protected static Configuration hBaseConfiguration = null;
    protected static HTable hTable = null;
    protected static FileWriter fileWriter = null;
    protected static List<Put> list = null;
    protected static int retentNum = 0;
    protected static int readNum = 0;
    protected static String logFile = null;
	    
    static {        
	    hBaseConfiguration = HBaseConfiguration.create();
	    hBaseConfiguration.set("hbase.zookeeper.quorum", "moses.namenode01,moses.datanode10,moses.datanode11,moses.datanode12,moses.datanode13");
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
    static void inject(String path, String field1, String field2) {
    	BufferedReader fR = null;
    	try {
    		String lineTemp;
    		fR = new BufferedReader(new FileReader(new File(path)));
    		while ((lineTemp = fR.readLine()) != null) {
    			String[] lineArray = lineTemp.split("\t");
    			String key;
    			String retentTemp;
    			String readTemp;
    			// gid 和 留存率 
    			try {
    				if(lineArray.length != 1) {
    					writeLog(lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				
    				key = lineArray[0];
    				retentTemp = "0";
    				readTemp = "0";
    				
    				addRow(hTable, key, "x", field1, retentTemp);
    				addRow(hTable, key, "x", field2, readTemp);
    				++ retentNum;
    				++ readNum;

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
		
		if(args.length != 5) {
			System.out.println("输入参数错误:\n请依次输入:留存率结果、日志文件、hbase表名、字段1、字段2");
			
			return;
		}

		String retentionPath = args[0];
		logFile = args[1];
		String tableName = args[2];
		String field1 = args[3];
		String field2 = args[4];
		
		System.out.println("inject file: " + retentionPath);
		System.out.println("log path: " + logFile);
		System.out.println("hbase table name: " + tableName);
		System.out.println("field1: " + field1);
		System.out.println("field2: " + field2);
		
		
		// 获取 hbase 的 htable;
		hTable = getHtable(tableName); //-----
		writeLog("start inject...", logFile);
		
		/* 检查字段是否合法 */
		if((!field1.equals("rn_d") || !field2.equals("rt_d"))
				&& (!field1.equals("rn_w") || !field2.equals("rt_w"))
				&& (!field1.equals("rn_w7") || !field2.equals("rt_w7"))) {
			System.out.println("输入hbase字段出错(或顺序出错)");
			writeLog("输入hbase字段出错(或顺序出错)", logFile);
			return;
		}
		
		inject(retentionPath, field1, field2);
		writeLog("留存率写入数量：" + retentNum, logFile);
		writeLog("阅读量写入数量：" + readNum, logFile);
		closeHbase();
		writeLog("inject complete!!!", logFile);
		
		System.out.println("ok");
	}

}
