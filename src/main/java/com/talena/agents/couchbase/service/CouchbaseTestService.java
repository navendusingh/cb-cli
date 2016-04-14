package com.talena.agents.couchbase.service;

import org.apache.log4j.Logger;

import com.talena.agents.couchbase.AuthInfo;
import com.talena.agents.couchbase.core.CouchbaseFacade;

public class CouchbaseTestService {
  private final static Logger logger = Logger.getLogger(
      CouchbaseTestService.class);

  private AuthInfo bucketAuthInfo;
  private String[] nodes;

  public CouchbaseTestService(
      final AuthInfo bucketAuthInfo, final String[] nodes) {
    this.bucketAuthInfo = new AuthInfo(bucketAuthInfo);
    this.nodes = nodes.clone();
  }

  public int getVBucketsCount() {
    CouchbaseFacade cbFacade = new CouchbaseFacade(
        nodes, bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    cbFacade.openBucket();

    int count = cbFacade.numPartitions();

    logger.info("Bucket: " + bucketAuthInfo.getName()
      + ". vBuckets: " + count);

    return count;
  }
}
