package org.hwx.hbase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.google.protobuf.ServiceException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimerTask;

//install hbase locally & hbase master start
public class HBaseClientExample extends TimerTask{

	private static final Logger LOG = LoggerFactory.getLogger(HBaseClientExample.class);
	private static Connection hbaseConn = null;

	private static Configuration hdpConfig = new Configuration();
	Set<Integer> metricGroups = new HashSet<Integer>();
	private final static Charset UTF8 = Charset.forName("UTF-8");
	String namespace;
	String tableName;
	String colFamily;
	String[] cols;
	
	
	public HBaseClientExample(Configuration config, String namespace, String tableName,String colFamily,String[] cols ) throws IOException, ServiceException {
		this.connect(config);
		this.namespace = "venkataw";
		this.tableName = "tabStudent";
		this.colFamily = "colfamStudentDetails";
		this.cols = cols;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			getRowsInHBase(namespace, tableName, colFamily);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		putRowInHBase(namespace, tableName, colFamily, cols);
	}
	private void connect(Configuration config) throws IOException, ServiceException {

		try {
			LOG.info("Establishing HBase Connection");
			HBaseAdmin.checkHBaseAvailable(config);
			hbaseConn = ConnectionFactory.createConnection(config);

		} catch (MasterNotRunningException e) {
			LOG.info("HBase is not running." + e.getMessage());
			e.printStackTrace();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}

		//HBaseClientOperations HBaseClientOperations = new HBaseClientOperations();
		//HBaseClientOperations.run(config);
	}
	
	private void getRowsInHBase(String namespace, String tableName, String colFamily) throws IOException
	{
		LOG.info("Reading HBase Table");
		Table table = hbaseConn.getTable(TableName.valueOf(namespace,tableName));
		LOG.info("HBase Table Name: "+table.getName().getNameAsString());

		LOG.info("Scanning HBase Table");
		Scan scan = new Scan();
		//colfamStudentDetails
		scan.addFamily(Bytes.toBytes(colFamily));
		ResultScanner rs = table.getScanner(scan);
		Iterator<Result> it = rs.iterator();
		while (it.hasNext()) {
			Result result = it.next();
			Integer rkey = new Integer(Bytes.toString(result.getRow()));
			LOG.info("row key: "+rkey);
			metricGroups.add(rkey);
			LOG.info("Scan Row Result: "+rkey);
		}
		table.close();

	}
	private void getRowInHBaseTable(String namespace, String tableName, String colFamily, String rowId,String colId) throws IOException {
		LOG.info("Reading HBase Table");
		Table table = hbaseConn.getTable(TableName.valueOf(namespace,tableName));
		LOG.info("HBase Table Name: "+table.getName().getNameAsString());

		LOG.info("Scanning HBase Table");
		Scan scan = new Scan();
		//colfamStudentDetails
		scan.addFamily(Bytes.toBytes(colFamily));
		Result r = table.get(new Get(Bytes.toBytes(rowId)));			
		byte [] value = r.getValue(Bytes.toBytes(colFamily),Bytes.toBytes(colId));
		String name = Bytes.toString(value);
		LOG.info("HBASE READ complete with get Row ID: "+name);
		table.close();
		
	}
	private  void putRowInHBase (String namespace, String tableName, String colFamily, String[] cols)
	{
		try {
		Table table = hbaseConn.getTable(TableName.valueOf(namespace,tableName));
		LOG.info("Writing into Table with Put");

		//Read Json String 
		String jsonString=null;
		String URI = "https://randomapi.com/api/443a77015a6d11e1770d7d38ede24765";
    	jsonString=ReadJsonURL.getJsonStringFromURI(URI);
    	
    	ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode=objectMapper.readTree(jsonString);
        String studentId = jsonNode.get(cols[0]).asText();
        String firstName = jsonNode.get(cols[1]).asText();
        
        LOG.info("ID inserted "+studentId);
        LOG.info("Name being inserted "+firstName);
        LOG.info("ColumnFamily: "+ colFamily+" Table " +table+" cols[0]" + cols[0] +"cols[1]" +cols[1]);
       
		Put put1 = new Put(Bytes.toBytes(studentId));
	    put1.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(cols[0]), Bytes.toBytes(studentId));
	    put1.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(cols[1]), Bytes.toBytes(firstName));
	    table.put(put1);
	    table.close();
	} catch (MasterNotRunningException e) {
		LOG.info("HBase is not running." + e.getMessage());
		e.printStackTrace();
	}
	catch (Exception ex) {
		ex.printStackTrace();
	}
	}
	private  void putRandomRowsInHBase(String namespace, String tableName, String colFamily, String[] cols)
	{
		LOG.info("Writing into Table with Put");
		try {
		Table table = hbaseConn.getTable(TableName.valueOf(namespace,tableName));
		LOG.info("Writing into Table with Put");
		
		Faker faker = new Faker();
		int maxRowKey=1, rowKeyCount=0;
		if(!Collections.max(metricGroups).equals(null))
		{
			maxRowKey = Collections.max(metricGroups);
		}


		while (rowKeyCount < 10) {
			maxRowKey++;
		    Put put1 = new Put(Bytes.toBytes(Integer.toString(maxRowKey)));
		    String firstName = faker.name().firstName();
		    LOG.info("FirstNameFaker: "+firstName);
			LOG.info("Age: "+maxRowKey);
			long startTime = System.currentTimeMillis();
			//code to be timed here
		    put1.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(cols[0]), Bytes.toBytes(maxRowKey));
		    put1.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(cols[1]), Bytes.toBytes(firstName));
		    table.put(put1);
		    long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			LOG.info("Elapsed Time:"+elapsedTime);
			
			rowKeyCount++;
		}
		
		table.close();
	} catch (MasterNotRunningException e) {
		LOG.info("HBase is not running." + e.getMessage());
		e.printStackTrace();
	}
	catch (Exception ex) {
		ex.printStackTrace();
	}
	}



}