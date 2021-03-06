package com.orientechnologies.binary.protocol.common;

public class Constants {

    public static final String SERIALIZATION_DOCUMENT2CSV = "ORecordDocument2csv";

    public static final String CLUSTER_TYPE_PHYSICAL = "PHYSICAL";

    public static final int REQUEST_SHUTDOWN = 1;

    public static final int REQUEST_CONNECT = 2;

    public static final int REQUEST_DB_OPEN = 3;

    public static final int REQUEST_DB_LIST = 74;

    public static final int REQUEST_DB_CLOSE = 5;

    public static final int DATA_CLUSTER_ADD_OP = 10;

    public static final int DATA_CLUSTER_COUNT_OP = 12;

    public static final int RECORD_CREATE_OP = 31;

    public static final int RECORD_LOAD_OP = 30;

    public static final int RECORD_UPDATE_OP = 32;

    public static final int RECORD_DELETE_OP = 33;

    public static final int COMMAND_OP = 41;

    public static final String DATABASE_TYPE_DOCUMENT = "document";

    public static final String RECORD_TYPE_DOCUMENT = "d";

    public static final String QUERY_SYNC = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery";

    public static final String QUERY_ASYNC = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery";

    public static final String QUERY_CMD = "com.orientechnologies.orient.core.sql.OCommandSQL";

    public static final String QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin";

    public static final String QUERY_SCRIPT = "com.orientechnologies.orient.core.command.script.OCommandScript";
}