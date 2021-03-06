package com.talena.agents.couchbase.commons;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.VersionMismatchException;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.talena.agents.couchbase.commons.constants.CouchbaseConstants;

/**
 * Long record contains metadata and data.
 * <p>
 * It implements Writable interface
 * for serialization. Since serialized objects are not to be compared,
 * WritableComparable is not required.
 * <p>
 * Implementing a comparable is not required for now because this class
 * will not be passed to the reducer.
 * 
 * @author Unascribed
 */
public class CouchbaseLongRecord extends CouchbaseShortRecord {

  private static final Log logger = LogFactory.getLog(
      CouchbaseLongRecord.class);

  // TODO: Check if using ByteBuf is a better idea.
  private byte[] content;
  private int contentLen = -1;
  protected int expiration;
  protected long revisionseqno;
  protected int lockTime;
  protected long cas;
  protected int flags;
  private static final int LENGTH = Integer.SIZE * 4 + Long.SIZE * 3;
  
  /**
   * Use {@link #create(MutationMessage)}, {@link #create(RemoveMessage)}.
   */
  protected CouchbaseLongRecord() {
    super();
  }
  
  public static CouchbaseLongRecord create() {
    return new CouchbaseLongRecord();
  }

  public static CouchbaseLongRecord create(MutationMessage msg,
      long partitionUuid) {
    CouchbaseLongRecord rec = new CouchbaseLongRecord();
    rec.set(msg);
    if (msg.content().hasArray()) {
      rec.content = msg.content().array();
    } else {
      rec.content = new byte[msg.content().capacity()];
      msg.content().getBytes(msg.content().readerIndex(), rec.content);
    }
    rec.expiration = msg.expiration();
    rec.revisionseqno = msg.revisionSequenceNumber();
    rec.lockTime = msg.lockTime();
    rec.cas = msg.cas();
    rec.flags = msg.flags();
    if (rec.content != null) {
      rec.contentLen = rec.content.length;
    }
    rec.partitionUuid(partitionUuid);
    return rec;
  }

  public static CouchbaseLongRecord create(RemoveMessage msg,
      long partitionUuid) {
    CouchbaseLongRecord rec = new CouchbaseLongRecord();
    rec.set(msg);
    rec.revisionseqno = msg.revisionSequenceNumber();
    rec.cas = msg.cas();
    rec.partitionUuid(partitionUuid);
    return rec;
  }

  public static CouchbaseLongRecord create(short partitionId,
      long partitionUuid, long seqno, int expiration, int revisionseqno,
      int lockTime, long cas, int flags, Type recType, String key,
      byte[] content) {

    CouchbaseLongRecord rec = new CouchbaseLongRecord();
    rec.partition = partitionId;
    rec.partitionUuid = partitionUuid;
    rec.seqno = seqno;
    rec.recType = recType;
    rec.expiration = expiration;
    rec.revisionseqno = revisionseqno;
    rec.lockTime = lockTime;
    rec.cas = cas;
    rec.flags = flags;
    rec.key(key);
    rec.content = content;
    if (content == null) {
      rec.contentLen = -1;
    } else {
      rec.contentLen = content.length;
    }
    return rec;
  }

  /**
   * @return The content
   */
  public byte[] content() {
    return content;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    try {
      super.write(out);
      out.writeByte(CouchbaseConstants.TYPE_SERIAL_VERSION);
      switch (CouchbaseConstants.TYPE_SERIAL_VERSION) {
        case 1:
          writeV1(out);
          break;
        default:
          throw new IOException(String.format("Invalid serial version %s",
              String.valueOf(CouchbaseConstants.TYPE_SERIAL_VERSION)));
      }
    } catch (IOException e) {
      logger.error("Error in record serialization.", e);
      throw e;
    }
  }

  private void writeV1(DataOutput out) throws IOException {
    out.writeInt(this.expiration);
    out.writeLong(this.revisionseqno);
    out.writeInt(this.lockTime);
    out.writeLong(this.cas);
    out.writeInt(flags);
    out.writeInt(this.contentLen);
    if (this.contentLen != -1) {
      out.write(this.content);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    try {
      super.readFields(in);
      byte version = in.readByte();
      if (version > CouchbaseConstants.TYPE_SERIAL_VERSION) {
        throw new VersionMismatchException(
            CouchbaseConstants.TYPE_SERIAL_VERSION, version);
      }
      switch (version) {
        case 1:
          readFieldsV1(in);
          break;
        default:
          throw new IOException(String.format("Invalid serial version %s",
              String.valueOf(version)));
      }
    } catch (IOException e) {
      logger.error("Error in record deserialization.", e);
      throw e;
    }
  }

  private void readFieldsV1(DataInput in) throws IOException {
    this.expiration = in.readInt();
    this.revisionseqno = in.readLong();
    this.lockTime = in.readInt();
    this.cas = in.readLong();
    this.flags = in.readInt();
    this.contentLen = in.readInt();
    this.content = null;
    if (this.contentLen != -1) {
      this.content = new byte[this.contentLen];
      in.readFully(this.content);
    }
  }

  public void set(CouchbaseLongRecord record) {

    super.set(record);
    this.recType = Type.UPDATE;
    this.expiration = record.expiration;
    this.revisionseqno = record.revisionseqno;
    this.lockTime = record.lockTime;
    this.cas = record.cas;
    this.flags = record.flags;
    this.content = record.content;
    this.contentLen = -1;
    if (this.content != null) {
      this.contentLen = this.content.length;
    }
  }

  public int expiration() {
    return expiration;
  }

  public long revisionseqno() {
    return revisionseqno;
  }

  public int lockTime() {
    return lockTime;
  }

  public long cas() {
    return cas;
  }

  public int flags() {
    return flags;
  }

  @Override
  public int hashCode() {
    return (int) this.seqno;
  }
  
  /**
   * Compares all fields.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof CouchbaseLongRecord)) {
      return false;
    }
    CouchbaseLongRecord that = (CouchbaseLongRecord) o;
    if (!super.equals(that))
      return false;
    if (this.expiration == that.expiration
        && this.revisionseqno == that.revisionseqno
        && this.lockTime == that.lockTime && this.cas == that.cas
        && this.flags == that.flags && this.contentLen == that.contentLen
        && Arrays.equals(this.content, that.content)) {

      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return new StringBuilder("\nCouchbaseLongRecord [content (hash)=")
        .append(Arrays.hashCode(content)).append(", contentLen=")
        .append(contentLen).append(", expiration=").append(expiration)
        .append(", revisionseqno=").append(revisionseqno).append(", lockTime=")
        .append(lockTime).append(", cas=").append(cas).append(", flags=")
        .append(flags).append(", partition=").append(partition)
        .append(", seqno=").append(seqno).append(", recType=").append(recType)
        .append(partitionUuid).append("]").toString();
  }
  
  public int length() {
    return CouchbaseShortRecord.LENGTH + LENGTH + this.contentLen;
  }
  
  public void clear() {
    super.clear();
    content = null;
    contentLen = -1;
    expiration = 0;
    revisionseqno = 0;
    lockTime = 0;
    cas = 0;
    flags = 0;
  }
}
