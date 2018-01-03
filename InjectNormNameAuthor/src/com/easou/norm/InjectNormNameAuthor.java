package com.easou.norm;

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

public class InjectNormNameAuthor {

	protected static Configuration hBaseConfiguration = null;
    protected static HTable hTable = null;
    protected static FileWriter fileWriter = null;
    protected static List<Put> list = null;
    protected static int itemNum = 0;
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
    		
    		System.out.println("����");
    		return null;
    	}

    	return hTable;
    }

    static void addRow(HTable htable, String row, String columnfamily, String column, String value) throws Exception {
        Put e = new Put(Bytes.toBytes(row));
        e.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column), Bytes.toBytes(value));
        list.add(e);
        //System.out.println("���!!!");
    }
	    
    static void commitHbase() {
    	
    	if (null == hTable) {
    		System.out.println("htable nullpointer error!!!");
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
	    

    static void inject(String path) {
    	BufferedReader fR = null;
    	try {
    		String lineTemp;
    		fR = new BufferedReader(new FileReader(new File(path)));
    		while ((lineTemp = fR.readLine()) != null) {
    			String[] lineArray = lineTemp.split("\t");
    			String gid;
    			String normName;
    			String normAuthor;
    			
    			try {	
    				if(lineArray.length != 3) {
    					writeLog(lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				
    				gid = lineArray[0];
    				normName = lineArray[1];
    				normAuthor = lineArray[2];
    				
    				if (!gid.equals("")) {
    					if (!normAuthor.equals("")) {
    						addRow(hTable, gid, "x", "norm_author", normAuthor);
    					}
    					if (!normName.equals("")){
        					addRow(hTable, gid, "x", "norm_name", normName);
    					}
    					++itemNum;
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
			System.out.println("please input injectFile、logPath、hbaseTable");
			return;
		}

		String normResult = args[0];
		logFile = args[1];
		String tableName = args[2];
		
		System.out.println("inject file: " + normResult);
		System.out.println("log path: " + logFile);
		System.out.println("hbase table name: " + tableName);
	
		hTable = getHtable(tableName); //-----
		writeLog("start inject...", logFile);
		inject(normResult);
		writeLog("inject item: " + itemNum, logFile);
		closeHbase();
		writeLog("inject complete!!!", logFile);
		
		System.out.println("ok");
	}


}
