package com.orientechnologies.binary.abstracts;

import com.orientechnologies.binary.protocol.binary.OrientSocket;
import com.orientechnologies.binary.protocol.binary.SocketTransport;
import com.orientechnologies.binary.protocol.binary.data.Record;
import com.orientechnologies.binary.protocol.binary.data.RecordId;
import com.orientechnologies.binary.protocol.binary.operations.Connect;
import com.orientechnologies.binary.protocol.binary.serialization.CSV;
import com.orientechnologies.binary.protocol.common.ConfigurableTrait;
import org.apache.commons.lang3.ArrayUtils;

import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Operation extends ConfigurableTrait {

    protected int opCode;

    private OrientSocket socket;

    private byte[] writeStack;

    private byte[] inputBuffer;

    private String outputBuffer;

    protected SocketTransport transport;

    public Operation(SocketTransport transport) throws Exception {
        this.transport = transport;
        this.socket = transport.getSocket();
        this.writeStack = new byte[]{};
    }

    public OrientSocket getSocket() {
        return socket;
    }

    protected abstract <T> T _read() throws Exception;

    protected abstract void _write() throws Exception;

    protected void _checkConditions(SocketTransport transport) {}

    public Operation prepare() throws Exception {
        this._checkConditions(transport);
        this._writeHeader();
        this._write();
        return this;
    }

    public Operation send() throws Exception {
        this.socket.write(this.writeStack);
        return this;
    }

    public <T> T getResponse() throws Exception {
        this._readHeader();
        T result = this._read();
        return result;
    }

    protected void _writeHeader() {
        this._writeInt(this.transport.getSessionId());
        String token = this.transport.getToken();

        if (!(this instanceof Connect) && this.transport.isRequestToken()) {
            this._writeString(token);
        }
    }

    protected void _readHeader() throws Exception {
        byte status = this._readByte();
        int sessionId = this._readInt();

        if (status == 1) {
            this._readByte();
            this._readError();
        }
    }

    protected void _readError() throws Exception {
        String type = this._readString();
        String message = this._readString();
        byte hasMore = this._readByte();

        if (hasMore == 1) {
            this._readError();
        } else {
            byte[] javaStackTrace = this._readBytes();
        }
        throw new RuntimeException(type + " : " + message);
    }

    protected void _writeInt(Integer value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        this.writeStack = ArrayUtils.addAll(this.writeStack, buffer.array());
    }

    protected void _writeByte(byte value) {
        this.writeStack = ArrayUtils.add(this.writeStack, value);
    }

    protected byte _readByte() throws Exception {
        byte[] read_ = this.socket.read(1);
        inputBuffer = ArrayUtils.addAll(inputBuffer, read_);
        ByteBuffer wrapped = ByteBuffer.wrap(read_);
        return wrapped.get();
    }

    protected short _readShort() throws Exception {
        byte[] read_ = this.socket.read(2);
        inputBuffer = ArrayUtils.addAll(inputBuffer, read_);
        ByteBuffer wrapped = ByteBuffer.wrap(read_);
        return wrapped.getShort();
    }

    protected char _readChar() throws Exception {
        byte[] read_ = this.socket.read(2);
        inputBuffer = ArrayUtils.addAll(inputBuffer, read_);
        ByteBuffer wrapped = ByteBuffer.wrap(read_);
        return wrapped.getChar();
    }

    protected byte[] _readBytes() throws Exception {
        int length = this._readInt();
        if (length == -1) {
            return null;
        } else {
            if (length == 0) {
                return new byte[]{};
            } else {
                byte[] read_ = this.socket.read(length);
                inputBuffer = ArrayUtils.addAll(inputBuffer, read_);
                return read_;
            }
        }
    }

    protected String _readSerialized() throws Exception {
        return this._readString();
    }

    protected void _writeString(String value) {
        this._writeInt(value.length());
        this.writeStack = ArrayUtils.addAll(this.writeStack, value.getBytes());
    }

    protected void _writeShort(Short value) {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort(value);
        this.writeStack = ArrayUtils.addAll(this.writeStack, buffer.array());
    }

    protected void _writeLong(Long value) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        this.writeStack = ArrayUtils.addAll(this.writeStack, buffer.array());
    }

    protected void _writeBoolean(Boolean value) {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(value ? (byte) 1: (byte) 0);
        this.writeStack = ArrayUtils.addAll(this.writeStack, buffer.array());
    }

    protected void _writeBytes(byte[] value) {
        if (value == null) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(-1);
            this.writeStack = ArrayUtils.addAll(this.writeStack, buffer.array());
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(value.length);
            this.writeStack = ArrayUtils.addAll(this.writeStack, ArrayUtils.addAll(buffer.array(), value));
        }
    }

    protected void _writeChar(char value) {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putChar(value);
        this.writeStack = ArrayUtils.addAll(this.writeStack, buffer.array());
    }

    protected Integer _readInt() throws Exception {
        byte[] read_ = this.socket.read(4);
        inputBuffer = ArrayUtils.addAll(inputBuffer, read_);
        ByteBuffer wrapped = ByteBuffer.wrap(read_);
        return wrapped.getInt();
    }

    protected String _readString() throws Exception {
        int length = this._readInt();

        if (length == -1) {
            return null;
        } else {
            if (length == 0) {
                return "";
            } else {
                byte[] read_ = this.socket.read(length);
                inputBuffer = ArrayUtils.addAll(inputBuffer, read_);
                return new String(read_);
            }
        }
    }

    protected Long _readLong() throws Exception {
        byte[] read_ = this.socket.read(8);
        this.inputBuffer = ArrayUtils.addAll(this.inputBuffer, read_);
        ByteBuffer wrapped = ByteBuffer.wrap(read_);
        return wrapped.getLong();
    }

    protected List<Record> _readPrefetchRecord() throws Exception {
        List<Record> resultSet = new ArrayList<>();
        byte status = this._readByte();

        while (status != 0) {
            Record record = this._readRecord();

            if (status == 1) {
                resultSet.add(record);
            }

            status = this._readByte();
        }
        return resultSet;
    }

    protected Record _readRecord() throws Exception {
        short classId = this._readShort();
        Map<String, Object> oRecord = new HashMap<>();
        oRecord.put("classId", classId);

        if (classId == -1) {
            throw new SocketException("No class for record, cannot proceed!");
        } else if (classId == -2) {
            oRecord.put("bytes", null);
        } else if (classId == -3) {
            oRecord.put("type", "d");
            short cluster = this._readShort();
            long position = this._readLong();
            oRecord.put("rid", new RecordId(cluster, (int) position));
        } else {
            oRecord.put("type", this._readChar());
            short cluster = this._readShort();
            long position = this._readLong();
            oRecord.put("version", this._readInt());

            Record data = CSV.deserialize(this._readString());
            oRecord.put("rid", new RecordId(cluster, (int) position));
            if (data.getoClass() != null) {
                oRecord.put("oClass", data.getoClass());
                data.setoClass(null);
            }
            oRecord.put("oData", data);
        }
        Record finalRecord = new Record();
        finalRecord.setoClass(oRecord.get("oClass").toString());
        finalRecord.setoData((Map<String, Object>) oRecord.get("oData"));
        finalRecord.setVersion(Integer.valueOf(oRecord.get("version").toString()));
        finalRecord.setRid((RecordId) oRecord.get("rid"));
        return finalRecord;
    }
}