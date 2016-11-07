package controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONObject;
import util.PropertyUtil;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * 解析Post请求数据，执行相应的Hbase操作，将操作结果状态封装成json返回给php后台。
 * 
 * @author Administrator
 */
public class HbaseDataOperation {
	private static Logger logger = Logger.getLogger(HbaseDataOperation.class);
	HBaseCommonUtil util;
	Map<String, HTable> tableMap;

	public HbaseDataOperation() {
		PropertyUtil.loadConfig();
		util = new HBaseCommonUtil(PropertyUtil.prop.getProperty("rootDir"),
				PropertyUtil.prop.getProperty("zookeeperAddress"), PropertyUtil.prop.getProperty("clientPort"));
		tableMap = new HashMap<String, HTable>();
	}

	public String compileJson(JSONObject dataJsonObj) {
		// 连接Hbase

		/**
		 * Post请求数据的表名
		 */
		String tableName = null;
		/**
		 * 数据应操作类型
		 */
		String operationType = null;
		/**
		 * Post请求数据的表中字段值
		 */
		String tableValue = null;
		/**
		 * Post请求数据的rowkey
		 */
		String rowKey = null;
		/**
		 * 操作状态结果返回Json中的ResultCode默认为1
		 */
		String ResultCode = "1";
		/**
		 * 操状状态结果返回Json中的报错信息Message默认为null
		 */
		StringBuffer Message = null;
		/**
		 * 返回Json
		 */
		String returnJson = null;

		tableName = dataJsonObj.get("tablename") + "";
		rowKey = dataJsonObj.get("rowkey") + "";
		operationType = dataJsonObj.get("type") + "";
		tableValue = dataJsonObj.get("value") + "";

		JSONObject valueJsonObj = JSONObject.fromObject(tableValue);
		long d = new Date().getTime();

		// 执行新增操作
		if (operationType.equals("INSERT")) {
			if (tableMap.get(tableName) == null) {
				try {
					tableMap.put(tableName, new HTable(util.getConf(), tableName));
				} catch (IOException e) {
					StringWriter sw = new StringWriter();
					e.printStackTrace(new PrintWriter(sw));
					ResultCode = "0";
					Message.append(sw.getBuffer());
					logger.error("处理错误|本次处理数据所属表名:  " + tableName + "本次处理数据的主键:  " + rowKey + "本次数据操作类型:  "
							+ operationType + "出错原因:	" + Message);
				}
			}
			HTable table = tableMap.get(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			Iterator it = valueJsonObj.keys();
			while (it.hasNext()) {
				String jsonkey = (String) it.next();
				put.add(Bytes.toBytes("detail"), Bytes.toBytes(jsonkey),
						Bytes.toBytes(valueJsonObj.getString(jsonkey)));
			}
			try {
				table.put(put);
			} catch (IOException e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				ResultCode = "0";
				Message.append(sw.getBuffer());
				logger.error("处理错误|本次处理数据所属表名:  " + tableName + "本次处理数据的主键:  " + rowKey + "本次数据操作类型:  " + operationType
						+ "出错原因:	" + Message);
			}
		}

		// 执行删除操作
		else if (operationType.equals("DELETE")) {
			if (tableMap.get(tableName) == null) {
				try {
					tableMap.put(tableName, new HTable(util.getConf(), tableName));
				} catch (IOException e) {
					StringWriter sw = new StringWriter();
					e.printStackTrace(new PrintWriter(sw));
					ResultCode = "0";
					Message.append(sw.getBuffer());
					logger.error("处理错误|本次处理数据所属表名:  " + tableName + "本次处理数据的主键:  " + rowKey + "本次数据操作类型:  "
							+ operationType + "出错原因:	" + Message);
				}
			}
			HTable table = tableMap.get(tableName);
			Delete del = new Delete(rowKey.getBytes());
			try {
				table.delete(del);
			} catch (IOException e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				ResultCode = "0";
				Message.append(sw.getBuffer());
				logger.error("处理错误|本次处理数据所属表名:  " + tableName + "本次处理数据的主键:  " + rowKey + "本次数据操作类型:  " + operationType
						+ "出错原因:	" + Message);
			}

		}

		// 执行更新操作
		else {
			if (tableMap.get(tableName) == null) {
				try {
					tableMap.put(tableName, new HTable(util.getConf(), tableName));
				} catch (IOException e) {
					StringWriter sw = new StringWriter();
					e.printStackTrace(new PrintWriter(sw));
					ResultCode = "0";
					Message.append(sw.getBuffer());
					logger.error("处理错误|本次处理数据所属表名:  " + tableName + "本次处理数据的主键:  " + rowKey + "本次数据操作类型:  "
							+ operationType + "出错原因:	" + Message);
				}
			}
			HTable table = tableMap.get(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			Iterator it = valueJsonObj.keys();
			while (it.hasNext()) {
				String jsonkey = (String) it.next();
				put.add(Bytes.toBytes("detail"), Bytes.toBytes(jsonkey),
						Bytes.toBytes(valueJsonObj.getString(jsonkey)));
			}
			try {
				table.put(put);
			} catch (IOException e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				ResultCode = "0";
				Message.append(sw.getBuffer());
				logger.error("处理错误|本次处理数据所属表名:  " + tableName + "本次处理数据的主键:  " + rowKey + "本次数据操作类型:  " + operationType
						+ "出错原因:	" + Message);
			}
		}

		returnJson = "{ResultCode:" + ResultCode + ",Message:" + Message + "}";
		return returnJson;
	}
}
