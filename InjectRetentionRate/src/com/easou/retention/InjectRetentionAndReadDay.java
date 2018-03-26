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
    					writeLog("inject retent:" + lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				
    				key = lineArray[0];
    				retentTemp = lineArray[5];
    				
    				retentF = Float.parseFloat(retentTemp);
    				if (retentF > 0 && retentF <= 1) {
        				addRow(hTable, key, "x", "rt_d", retentTemp);
        				
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
    			// gid 和 阅读量 
    			try {
    				if(lineArray.length != 2) {
    					writeLog("inject num: " + lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				
    				key = lineArray[0];
    				readTemp = lineArray[7];
    				readI = Integer.parseInt(readTemp);
    				if (readI > 0) {
    					addRow(hTable, key, "x", "rn_d", readTemp);
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
		
		if(args.length != 4) {
			System.out.println("输入参数错误:\n请依次输入:留存率结果、阅读量结果、日志文件、hbase表名");
			
			return;
		}

		String retentionPath = args[0];
		String readNumPath = args[1];
		logFile = args[2];
		String tableName = args[3];
		
		System.out.println("rt_d file: " + retentionPath);
		System.out.println("rt_d file: " + readNumPath);
		System.out.println("log path: " + logFile);
		System.out.println("hbase table name: " + tableName);
	
		// 获取 hbase 的 htable;
		hTable = getHtable(tableName); //-----
		writeLog("start inject...", logFile);
		injectRetent(retentionPath);
		injectReadNum(readNumPath);
		writeLog("留存率写入数量：" + retentNum, logFile);
		writeLog("阅读量写入数量：" + readNum, logFile);
		closeHbase();
		writeLog("inject complete!!!", logFile);
		System.out.println("ok");
	}

}
