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
	    list = new LinkedList<Put>();
    }
	    
    @SuppressWarnings("deprecation")
	static HTable getHtable(String tableName) {

    	try {
    		hTable = new HTable(hBaseConfiguration, tableName);
    	} catch (IOException e) {
    		e.printStackTrace();
    		System.out.println("错误!!!");
    		return null;
    	}

    	return hTable;
    }

    @SuppressWarnings("deprecation")
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
    			String normSeries;    			
    			try {	
    				if(lineArray.length != 4) {
    					writeLog(lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				
    				gid = lineArray[0];
    				normName = lineArray[1];
    				normAuthor = lineArray[2];
    				normSeries = lineArray[3];
    				
    				if ((!gid.equals("")) && (gid.startsWith("i_"))) {
    					if (!normAuthor.equals("")) {
    						addRow(hTable, gid, "x", "norm_author", normAuthor);
    					} else {
    						continue;
						}
    					if (!normName.equals("")){
        					addRow(hTable, gid, "x", "norm_name", normName);
    					} else {
    						continue;
						}
    					if (!normSeries.equals("")){
        					addRow(hTable, gid, "x", "norm_series", normSeries);
    					} else {
    						continue;
						}
    					++itemNum;
    				}
					if(list.size() >= 8192) {
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
			System.out.println("please input injectFile、logPath、hbaseTable");
			return;
		}
		// dDAU 

		String normResult = args[0];
		logFile = args[1];
		String tableName = args[2];
		String zookeeper = args[3];
		String zookeeperClient = args[4];
		
		System.out.println("inject file: " + normResult);
		System.out.println("log path: " + logFile);
		System.out.println("hbase table name: " + tableName);


		hBaseConfiguration.set("hbase.zookeeper.quorum", zookeeper);
		hBaseConfiguration.set("hbase.zookeeper.property.clientPort", zookeeperClient);

		hTable = getHtable(tableName); //-----
		writeLog("start inject...", logFile);
		inject(normResult);
		writeLog("inject item: " + itemNum, logFile);
		closeHbase();
		writeLog("inject complete!!!", logFile);
		
		System.out.println("ok");
	}


}
