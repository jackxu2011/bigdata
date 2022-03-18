package me.xuling.geek.bigdata.week02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author jack
 * @since 2022/3/18
 **/
public class HbaseUtils {

    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();

        //configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        //configuration.set("hbase.master", "192.168.80.133:16000");

        HbaseUtils hbaseUtils = new HbaseUtils();
        hbaseUtils.buildConnection(configuration, "jack:student");

        hbaseUtils.createTable( "name", "info", "score");
        Student student1 = new Student()
                .withStudentId("20210000000001")
                .withName("Tom")
                .withClassNo(1)
                .withUnderstandingScore(75)
                .withProgrammingScore(82);
        Student student2 = new Student()
                .withStudentId("20210000000002")
                .withName("Jerry")
                .withClassNo(1)
                .withUnderstandingScore(85)
                .withProgrammingScore(67);
        Student student3 = new Student()
                .withStudentId("20210000000003")
                .withName("Jack")
                .withClassNo(2)
                .withUnderstandingScore(80)
                .withProgrammingScore(80);
        Student student4 = new Student()
                .withStudentId("20210000000004")
                .withName("Rose")
                .withClassNo(2)
                .withUnderstandingScore(60)
                .withProgrammingScore(61);
        Student student5 = new Student()
                .withStudentId("G20220735020032")
                .withName("许灵")
                .withClassNo(2)
                .withUnderstandingScore(60)
                .withProgrammingScore(61);
        hbaseUtils.put(student1);
        hbaseUtils.put(student2);
        hbaseUtils.put(student3);
        hbaseUtils.put(student4);
        hbaseUtils.put(student5);
        Student result = hbaseUtils.get("G20220735020032");
        System.out.println("search studentId G20220735020032's name is " + result.getName());
        hbaseUtils.delete("20210000000004");
    }

    private Connection conn;

    private TableName tableName;

    public HbaseUtils() {
    }

    public void buildConnection(Configuration configuration, String tableName) throws IOException {
        this.conn = ConnectionFactory.createConnection(configuration);
        this.tableName = TableName.valueOf(tableName);
    }

    public void createTable(String... colFamilies) throws IOException {
        Admin admin = conn.getAdmin();

        NamespaceDescriptor nameSpace = NamespaceDescriptor.create(tableName.getNamespaceAsString()).build();
        try {
            admin.getNamespaceDescriptor(tableName.getNamespaceAsString());
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(nameSpace);
            System.out.println("nameSpace created!");
        }
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            for (String colFamily : colFamilies) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder
                        .newBuilder(colFamily.getBytes(StandardCharsets.UTF_8));
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }

            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("Table create successful");
        }
    }

    public void put(Student student) throws IOException {
        // 插入数据
        Put put = new Put(student.getRowKey());
        put.addColumn(Bytes.toBytes("name"), null,  Bytes.toBytes(student.getName()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes(student.getStudentId()));
        if (student.getClassNo() != null) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes(student.getClassNo()));
        }
        if (student.getUnderstandingScore() != null) {
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes(student.getUnderstandingScore()));
        }
        if (student.getClassNo() != null) {
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes(student.getClassNo()));
        }
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");
    }

    public Student get(String studentId) throws IOException {
        Student student = new Student();
        student.setStudentId(studentId);
        Get get = new Get(student.getRowKey());
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                if ("name".equals(cf)) {
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    student.setName(value);
                }
                if ("info".equals(cf)) {
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    if ("class".equals(colName)) {
                        Integer value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        student.setClassNo(value);
                    }
                }
                if ("score".equals(cf)) {
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    if ("understanding".equals(colName)) {
                        Integer value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        student.setUnderstandingScore(value);
                    }
                    if ("programming".equals(colName)) {
                        Integer value = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        student.setProgrammingScore(value);
                    }
                }
            }
        }
        return student;
    }

    public void delete(String studentId) throws IOException {
        Student student = new Student();
        student.setStudentId(studentId);
        Delete delete = new Delete(student.getRowKey());
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");
    }
    public void deleteTable() throws IOException {
        Admin admin = conn.getAdmin();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
