package controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Java与Hbase交互
 * 
 * @author Administrator
 *
 */
public class HBaseCommonUtil {
	private Configuration HBASE_CONFIG;
	private Configuration conf;
	private Connection conn;
	private HBaseAdmin hbaseAdmin;

	// 连接Hbase参数配置
	public HBaseCommonUtil(String rootDir, String zooKeeperAddress, String clientPort) {
		HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.rootdir", rootDir);
		HBASE_CONFIG.set("hbase.zookeeper.quorum", zooKeeperAddress);
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", clientPort);
		conf = HBaseConfiguration.create(HBASE_CONFIG);
		try {
			conn = ConnectionFactory.createConnection(conf);
			hbaseAdmin = (HBaseAdmin) conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
