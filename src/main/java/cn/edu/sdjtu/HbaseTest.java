package cn.edu.sdjtu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
	private static Connection connection;
	private static Admin admin;

	public static void createTable(String tableName, String[] fields) throws IOException {
		if (admin.tableExists(TableName.valueOf(tableName))) {
			deleteTable(tableName);
		}
		// TableDescriptorBuilder.newBuilder构建表描述构建器
		TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
		for (int i = 0; i < fields.length; i++) {
			// 创建列簇构造描述器
			ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder
					.newBuilder(Bytes.toBytes(fields[i]));
			// 构建列簇描述
			ColumnFamilyDescriptor cfDes = columnFamilyDescriptorBuilder.build();
			// 建立表与列簇的关联关系
			tableDescriptorBuilder.setColumnFamily(cfDes);
		}
		TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
		admin.createTable(tableDescriptor);
	}

	// 添加数据
	public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		String[] column = new String[110];
		String columnFamily = "";
		for (int i = 0; i < fields.length; i++) {
			String[] split = fields[i].split(":");
			column[i] = split[1];
			columnFamily = split[0];
		}
		Put put = new Put(Bytes.toBytes(row));
		for (int i = 0; i < values.length; i++) {
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column[i]), Bytes.toBytes(values[i]));
		}
		// 5.使用htable表执行put操作
		table.put(put);
		// 关闭htable表对象
		table.close();
	}

	public static void scanColumn(String tableName, String rowKey, String column) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowKey.getBytes());
		if (column.contains(":")) {
			// 查询指定rowkey和列簇下的指定列名
			String[] split = column.split(":");
			get.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
			Result result = table.get(get);
			byte[] value = result.getValue(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]));
			if (Bytes.toString(value) != null)
				System.out.println(Bytes.toString(value));
			else
				System.out.println("null");
		} else {
			// 查询指定rowkey和列簇下的所有数据
			get.addFamily(column.getBytes());
			Result result = table.get(get);
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				// 获取列簇名称
				String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
				// 获取列的名称
				String colunmName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength());
				// 获取值
				String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				if (value != null)
					System.out.println(cf + ":" + colunmName + "=>" + value);
				else
					System.out.println(cf + ":" + colunmName + "=>" + "null");
			}
		}
		table.close();
	}

	public static void modifyData(String tableName, String rowkey, String column, String value) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		// 修改操作
		Put put = new Put(Bytes.toBytes(rowkey));
		String[] split = column.split(":");
		String columnFamily = split[0];
		String columnName = split[1];
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
		table.put(put);
		// 查看修改后的数据
		Get get = new Get(rowkey.getBytes());
		get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
		Result result = table.get(get);
		byte[] value2 = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
		if (Bytes.toString(value2) != null)
			System.out.println(columnFamily + ":" + columnName + "=>" + Bytes.toString(value2));
		else
			System.out.println("null");
		System.out.println("修改成功！！");
		table.close();
	}

	public static void deleteRow(String tableName, String row) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		// 删除一条rowkey记录
		Delete delete = new Delete(Bytes.toBytes(row));
		table.delete(delete);
		table.close();
	}

	public static void deleteTable(String tablename) throws IOException {
		TableName tableName = TableName.valueOf("WATER_BILL");
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}
	 public static FSDataInputStream readFile(FileSystem fs, String path){
	        try {
	            FSDataInputStream his = fs.open(new Path(path));
	            return his;
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
			return null;
	    }
	public static void main(String[] args) throws Exception{
		//用HBaseConfiguration.create();创建HBase的配置
        Configuration configuration = HBaseConfiguration.create();
        //用ConnectionFactory.createConnection创建HBase()连接
        connection = ConnectionFactory.createConnection(configuration);
        // 创建表，要给予HBase获取Admin对象
        admin = connection.getAdmin();
        String tablename = "identify_rmb_users";
        //创建表和列
        String[] filed = {"Info"};
        createTable(tablename, filed);
        //创建configuration对象
        Configuration conf = new Configuration();
        //配置在etc/hadoop/core-site.xml   fs.defaultFS
        conf.set("fs.defaultFS","hdfs://1192.168.26.132:9000");
        //创建FileSystem对象
        FileSystem fs = FileSystem.get(conf);
        
        String[] fields = {"Info:idcard","Info:name", "Info:gender", "Info:bank", "Info:address", "Info:phone", "Info:birthday"};
        
        FSDataInputStream his = readFile(fs,"hdfs://192.168.26.132:9000/mapreduce/uid_details.txt");
        byte[] buff = new byte[1024];
        int length = 0;
        while ((length = his.read(buff)) != -1) {
         String line = new String(buff, 0, length);
         String[] values = line.split(",");
         String row = values[0];
         
         addRecord(tablename, row, fields, values);
        }
        
        String column = "Info:gender";
        String rowkey = "female";
        scanColumn(tablename, rowkey, column);
        
        String column2 = "Info:bank";
        String rowkey2 = "COMMCN";
        scanColumn(tablename, rowkey2, column2);
        
	}
}
