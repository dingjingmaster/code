package com.easou.retention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InjectRetentAndBuyDay {
	protected static Configuration hBaseConfiguration = null;
    protected static HTable hTable = null;
    protected static FileWriter fileWriter = null;
    protected static List<Put> list = null;
    protected static int retentNum = 0;
    protected static int buyNum = 0;
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
    		System.out.println("htable ����!!!");
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
     *	path Ң������ 
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
    			// gid �� ������ 
    			try {
    				if(lineArray.length != 8) {
    					writeLog("inject retent:" + lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				
    				key = lineArray[0];
    				retentTemp = lineArray[5];
    				
    				retentF = Float.parseFloat(retentTemp);
    				if (retentF > 0 && retentF <= 1) {
        				addRow(hTable, key, "x", "bt_d", retentTemp);
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
    
    static void injectBuyNum(String path) {
    	BufferedReader fR = null;
    	try {
    		String lineTemp;
    		fR = new BufferedReader(new FileReader(new File(path)));
    		while ((lineTemp = fR.readLine()) != null) {
    			String[] lineArray = lineTemp.split("\t");
    			String key;
    			String buyTemp;
    			int buyI;
    			// gid �� �Ķ��� 
    			try {
    				if(lineArray.length != 2) {
    					writeLog("inject num: " + lineTemp + "\twrong length", logFile);
    					continue;
    				}
    				
    				key = lineArray[0];
    				buyTemp = lineArray[1];
    				buyI = Integer.parseInt(buyTemp);
    				if (buyI > 0) {
    					addRow(hTable, key, "x", "bn_d", buyTemp);
    					++ buyNum;
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
			System.out.println("");
			return;
		}

		String retentionPath = args[0];
		String buyNumPath = args[1];
		logFile = args[2];
		String tableName = args[3];
		String zookeeper = args[4];
		String zookeeperClient = args[5];

		System.out.println("bt_d file: " + retentionPath);
		System.out.println("bn_d file: " + buyNumPath);
		System.out.println("log path: " + logFile);
		System.out.println("hbase table name: " + tableName);

		hBaseConfiguration.set("hbase.zookeeper.quorum", zookeeper);
		hBaseConfiguration.set("hbase.zookeeper.property.clientPort", zookeeperClient);

		// hbase htable;
		hTable = getHtable(tableName); //-----
		writeLog("start inject...", logFile);
		injectRetent(retentionPath);
		injectBuyNum(buyNumPath);
		writeLog("" + retentNum, logFile);
		writeLog("" + buyNum, logFile);
		closeHbase();
		writeLog("inject complete!!!", logFile);
		System.out.println("ok");
	}

}