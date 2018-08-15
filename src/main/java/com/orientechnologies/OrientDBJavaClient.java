package com.orientechnologies;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrientDBJavaClient {
    private final ODatabaseDocumentTx connection;

    public OrientDBJavaClient(String url, String username, String password) {
        connection = new OPartitionedDatabasePool(url, username, password).acquire();
    }

    public int createCluster(String clusterName) {
        if (!connection.existsCluster(clusterName)) {
            connection.addCluster(clusterName);
            return connection.getClusterIdByName(clusterName);
        } else {
            return connection.getClusterIdByName(clusterName);
        }
    }

    public void createDocument(String cluster, Map<String, String> props) {
        ODocument document = new ODocument();

        for (Map.Entry<String, String> prop: props.entrySet()) {
            document.field(prop.getKey(), prop.getValue(), OType.STRING);
        }
        connection.save(document, cluster);
    }

    public void readDocuments(String cluster) {
        List<ODocument> documents = connection.query(new OSQLSynchQuery<>("select * from cluster:" + cluster));

        if (documents.size() == 0) {
            System.out.println("No Documents");
        }

        System.out.println("Documents present: " + documents.size());
        System.out.println(documents.get(1).getRecord().getIdentity().getClusterPosition());
    }

    public static void main(String[] args) {
        OrientDBJavaClient javaClient = new OrientDBJavaClient("remote:127.0.0.1:2424/GratefulDeadConcerts",
                "root", "root");

        String clusterName = "testcluster123";

        int clusterId = javaClient.createCluster(clusterName);
        System.out.println("Cluster Id: " + clusterId);

        Map<String, String> props1 = new HashMap<>();
        props1.put("name", "Subhobrata" + new java.util.Random().nextInt());
        props1.put("surname", "Dey");

        Map<String, String> props2 = new HashMap<>();
        props2.put("name", "Priyadarshini" + new java.util.Random().nextInt());
        props2.put("surname", "Das");

        javaClient.createDocument(clusterName, props1);
        javaClient.createDocument(clusterName, props2);

        javaClient.readDocuments(clusterName);
    }
}