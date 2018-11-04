package com.orientechnologies.binary;

import com.orientechnologies.binary.protocol.JavaOrient;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.data.RecordId;

import java.util.Collections;
import java.util.Map;

public class JavaOrientTest {

    private String hostname;
    private String port;

    private String username;
    private String password;

    private final JavaOrient javaOrient;

    public JavaOrientTest(String hostname, String port,
                          String username, String password) {
        this.hostname = hostname;
        this.port = port;

        this.username = username;
        this.password = password;

        javaOrient = new JavaOrient(this.hostname, this.port);
    }

    public void connect() {
        javaOrient.connect(this.username, this.password);
    }

    public Map<String, String> dbList() {
        return javaOrient.dbList(this.username, this.password);
    }

    public void dbOpen(String database) {
        javaOrient.dbOpen(database, this.username, this.password);
    }

    public short createCluster(String clusterName) {
        return javaOrient.addCluster(clusterName);
    }

    public Integer getCluster(String clusterName) {
        return javaOrient.getCluster(clusterName);
    }

    public Record createRecord(Record record) {
        return javaOrient.createRecord(record);
    }

    public Record readRecord(RecordId recordId) {
        return javaOrient.readRecord(recordId.getCluster(), (long) recordId.getPosition(), null);
    }

    public Record updateRecord(Record record, RecordId recordId) {
        return javaOrient.updateRecord(record, recordId);
    }

    public boolean deleteRecord(RecordId recordId) {
        return javaOrient.deleteRecord(recordId);
    }

    public void dbClose() {
        javaOrient.dbClose(this.username, this.password);
    }

    public void shutdown() {
        javaOrient.shutDown(this.username, this.password);
    }

    public static void main(String[] args) {
        JavaOrientTest javaOrientTest = new JavaOrientTest("127.0.0.1", "2424",
                "root", "root");

        javaOrientTest.connect();

        Map<String, String> dbList = javaOrientTest.dbList();
        for (Map.Entry<String, String> db: dbList.entrySet()) {
            System.out.println(db.getKey() + " - " + db.getValue());
        }

        javaOrientTest.dbOpen("GratefulDeadConcerts");

        short cluster = javaOrientTest.createCluster("orient_test_cluster");

        cluster = javaOrientTest.getCluster("orient_test_cluster").shortValue();

        Record record = new Record();
        record.setVersion(1);
        record.setRid(new RecordId(cluster, -1));
        record.setoData(Collections.singletonMap("hello", "world"));
        record.setoClass("orientclass");

        Record createdRecord = javaOrientTest.createRecord(record);

        Record readRecord = javaOrientTest.readRecord(createdRecord.getRid());
        System.out.println(readRecord.getoData().get("hello"));

        record.setoData(Collections.singletonMap("hello", "orient"));
        Record updateRecord = javaOrientTest.updateRecord(record, createdRecord.getRid());

        readRecord = javaOrientTest.readRecord(updateRecord.getRid());
        System.out.println(readRecord.getoData().get("hello"));

        javaOrientTest.deleteRecord(readRecord.getRid());

        javaOrientTest.dbClose();
    }
}