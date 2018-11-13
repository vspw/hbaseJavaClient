package org.hwx.hbase;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.security.PrivilegedExceptionAction;
import java.util.Timer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Purpose: To Schedule HBase client to get and put hbase rows
 * Usage:
 * Requires HBASE_CONF_DIR and HADOOP_CONF_DIR on the Classpath
 * OR
 * Add the configuration files explicitly.
 * Arguments: -n venkataw -t tabStudent -c colfamStudentDetails -o id name -u test@HWX.COM -k C:\\Users\\VenkataW\\Documents\\repo\\krb5.keytab -q 5
 * Environment Variables: HBASE_CONF_DIR and HADOOP_CONF_DIR (added in eclipse - run configurations - environment)
 * OR
 * Use faker library to generate random data.
 * Edit the namespace, table name, column family and column names
 * 
 * java -Dorg.hwx.hbase.HBaseClientScheduler -Dlog4j.configuration=file:///home/venkata/hbase/log4j.properties 
 * -cp /usr/hdp/current/hadoop-client/client/*:./hwx-hbase-client-0.0.1-SNAPSHOT.jar:/usr/hdp/current/hbase-client/lib/* org.hwx.hbase.HBaseClientScheduler 
 * -n venkataw -t tabStudent -c colfamStudentDetails -o id name -u test@hwx.COM -k /home/venkata/hbase/TEST.keytab -q 5
 */

public class HBaseClientScheduler {
private static final Logger LOG = LoggerFactory.getLogger(HBaseClientScheduler.class);
private static UserGroupInformation ugi=null;

public static void main(String[] args)
{
	try
	{
        Options options = new Options();

        Option ns = new Option("n", "namespace", true, "HBase Table Namespace");
        ns.setRequired(false);
        options.addOption(ns);
        
        Option table = new Option("t", "table", true, "HBase Table Name");
        table.setRequired(true);
        options.addOption(table);
        
        Option colFam = new Option("c", "columnfamily", true, "HBase Column Family");
        colFam.setRequired(true);
        options.addOption(colFam);
        
        Option arrCols = new Option("o", "columns", true, "HBase Column Names Array");
        arrCols.setArgs(10);
        arrCols.setRequired(true);
        options.addOption(arrCols);
        

        Option upn = new Option("u", "upn", true, "User principal for HBASE operations");
        upn.setRequired(true);
        options.addOption(upn);

        Option keytab = new Option("k", "keytab", true, "Keytab file path for HBASE authentication");
        keytab.setRequired(true);
        options.addOption(keytab);
        
        Option freq = new Option("q", "freq", true, "The frequency in seconds for repeat period of application");
        freq.setRequired(true);
        options.addOption(freq);
        
        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOG.info(e.getMessage());
            
            Writer buffer = new StringWriter();
            PrintWriter pw = new PrintWriter(buffer);
            formatter.printUsage(pw, 512, "java -Dlog4j.configuration=file:<log4jfile> -cp <hadoop_client_libs>" + HBaseClientScheduler.class.getName(), options);
            LOG.info(buffer.toString());
            pw.flush();
            buffer.close();
            pw.close();
            System.exit(1);
            return;
        }

        final String inputFilePath = cmd.getOptionValue("input");
        final String upnAuth = cmd.getOptionValue("upn");
        final String keytabAuth = cmd.getOptionValue("keytab");
        final int freqSeconds = Integer.parseInt(cmd.getOptionValue("freq"));
        final String strNamespace = cmd.getOptionValue("namespace");
        final String strTable = cmd.getOptionValue("table");
        final String strColumnFamily = cmd.getOptionValue("columnfamily");
        final String[] strArrCols = cmd.getOptionValues("columns");

        LOG.info("InputFilePath: "+inputFilePath);
        LOG.info("upnAuth: "+upnAuth);
        LOG.info("keytabAuth: "+keytabAuth);
        LOG.info("freqSeconds: "+freqSeconds);
        
        
	
	LOG.info("Establishing Connection to HDFS. This initializes the UGI for future processing.");
	LOG.info("Adding Configuration files for HDFS, HBASE");
	Configuration config = HBaseConfiguration.create();
//	config.addResource(new Path("C:\\Users\\USER\\eclipse-workspace\\hwx-hbase-client\\src\\main\\resources\\hdfs-site.xml"));
//	config.addResource(new Path("C:\\Users\\USER\\eclipse-workspace\\hwx-hbase-client\\src\\main\\resources\\core-site.xml"));
//	config.addResource(new Path("C:\\Users\\USER\\eclipse-workspace\\hwx-hbase-client\\src\\main\\resources\\hbase-site.xml"));
    config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
    config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
	UserGroupInformation.setConfiguration(config); 
	
	LOG.info("Setting up UGI using Keytab");
	
	ugi=UserGroupInformation.loginUserFromKeytabAndReturnUGI(upnAuth,keytabAuth);
	UserGroupInformation.setLoginUser(ugi);
	ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
	
	//Display UGI Details
	LOG.info("IsSecurityEnabled: "+UserGroupInformation.isSecurityEnabled());
	//System.out.println(ugi.);
	LOG.info("Is UGI from Keytab: "+ugi.isFromKeytab());
	//System.out.println(UserGroupInformation.getLoginUser());
	LOG.info("UGI Username: "+ugi.getUserName());
	ugi.doAs(new PrivilegedExceptionAction<Void>() {

		public Void run() throws Exception {
			LOG.info("Start the Timer threads with the input repeatPeriod (seconds)");
			Timer timer = new Timer();
			//String cols[]= {"id","name"};
			timer.schedule(new HBaseClientExample(config,strNamespace,strTable,strColumnFamily,strArrCols), 0, freqSeconds*1000);

			return null;
		}
	});
    
	}
	catch(Exception ex)
	{
		ex.printStackTrace();
	}
}
}
