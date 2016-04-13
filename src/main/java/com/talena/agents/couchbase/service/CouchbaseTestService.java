package com.talena.agents.couchbase.service;

import org.apache.log4j.Logger;

import com.talena.agents.couchbase.BucketAuthInfo;
import com.talena.agents.couchbase.core.CouchbaseFacade;

public class CouchbaseTestService {
  private final static Logger logger = Logger.getLogger(
      CouchbaseTestService.class);

  private BucketAuthInfo bucketAuthInfo;
  private String[] nodes;

  public CouchbaseTestService(
      final BucketAuthInfo bucketAuthInfo, final String[] nodes) {
    this.bucketAuthInfo = new BucketAuthInfo(bucketAuthInfo);
    this.nodes = nodes.clone();
  }

  public int getVBucketsCount() {
    CouchbaseFacade cbFacade = new CouchbaseFacade(
        nodes, bucketAuthInfo.getBucketName(), bucketAuthInfo.getPassword());

    cbFacade.openBucket();

    int count = cbFacade.numPartitions();

    logger.info("Bucket: " + bucketAuthInfo.getBucketName()
      + ". vBuckets: " + count);

    return count;
  }
}
