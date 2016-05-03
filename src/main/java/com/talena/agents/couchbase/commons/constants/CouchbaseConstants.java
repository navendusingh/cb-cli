package com.talena.agents.couchbase.commons.constants;

public class CouchbaseConstants {

  public static final String CLUSTER_ID = "clusterId";
  public static final String CLUSTER_ID_DESC = "ID of the Couchbase cluster.";
  public static final String BACKUP_ID = "backupId";
  public static final String BACKUP_ID_DESC = "ID of the backup job.";
  public static final String SEEDS = "seeds";
  public static final String SEEDS_DESC =
      "Comma separated list of seed node addresses of Couchbase server.";

  public static final String BUCKETS = "buckets";
  public static final String BUCKETS_DESC = "Comma separated list of buckets.";

  public static final String BUCKETS_AUTHINFO = "bucketsAuthInfo";
  public static final String BUCKETS_AUTHINFO_DESC =
      "Serialized Map of AuthInfo of Buckets";

  public static final String USERNAME = "username";
  public static final String USERNAME_DESC =
      "Username for authentication with Couchbase server.";

  public static final String PASSWORD = "password";
  public static final String PASSWORD_DESC =
      "Password for authentication with Couchbase server.";

  /**
   *  The serialization version of types used in Couchbase agent. All types
   *  share this version and when it is incremented, all types are affected.
   *  This is because a) having a version for each type is error prone when
   *  one type has a reference to another and b) having a single version makes
   *  it possible in some cases to not include version in the serialized form.
   */
  public static final byte TYPE_SERIAL_VERSION = 1;

  public static final String SEQ_FILE_META_KEY = "couchbase.seq.meta.key";

  // info required for backup input split
  public static final String BUCKET_COUNT = "bucketCount";
  public static final String MAX_PARTITIONS_PER_SPLIT =
      "max.partitions.per.split";
  /** Config key to specify buffer size for records. */
  public static final String BUFFER_SIZE = "Couchbase.buffer.size";
  
  /** Config key to specify seed nodes. */
  public static final String SEED_NODES = "Couchbase.seed.nodes";
  
  /** Config key for buckets. */
  public static final String CONF_BUCKETS = "couchbase.buckets";
  /** Config key under which base directory is stored. */
  public static final String BASE_DIR = "Couchbase.base.dir";
  /** Config key under which file sequence number is kept. */
  public static final String FILE_SEQ_NO = "Couchbase.fseqno";
  /** 
   * Config key to set frequency of progress report in terms of number of data
   * mutations. For example, 100.
   */
  public static final String PROGRESS_FQ = "Couchbase.progress.fq";
  
  public static final String AGENT_SUCCESS_FILE_NAME =
      "couchbase.success.file.name";
  /**
   * The key under which number of partitions per buckets is stored in the
   * configuration. The value must be fetched from Couchbase server.
   */
  public static final String PARTITIONS_PER_BUCKET =
      "couchbase.partitions.per.bucket";
  /**
   * Config key to set for how long the record reader should poll the queue
   * for before giving up and throwing exception.
   */
  public static final String REC_READER_GIVEUP_TIMEOUT_SECS =
      "couchbase.rec.reader.giveup.secs";

  // catalog avro file map keys
  public static final String PARTITION_RESULT_PARTITIONID = "partition.result.partitionid";
  public static final String PARTITION_RESULT_PARTITIONUUID = "partition.result.partitionuuid";
  public static final String PARTITION_RESULT_HIGHSEQNO = "partition.result.highseqno";
  public static final String PARTITION_RESULT_SUCCESS = "partition.result.success";
  
  public static final String SEQUENCE_FILE_COMPRESSION_KEY =
      "Couchbase.seq.file.compression";
  
  public static final String SEQUENCE_FILE_COMPRESSION_RECORD =
      "RECORD";
  
  public static final String SEQUENCE_FILE_COMPRESSION_BLOCK =
      "BLOCK";
  
  public static final String SEQUENCE_FILE_COMPRESSION_NONE =
      "NONE";
}
