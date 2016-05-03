package com.talena.agents.couchbase.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.talena.agents.couchbase.AuthInfo;
import com.talena.agents.couchbase.commons.CouchbaseLongRecord;

public class RestoreService {
  private final static Logger logger = Logger.getLogger(RestoreService.class);

  private int bufferMaxItems;
  private long bufferMaxSize;

  private long sizeInBuffer;

  private List<CouchbaseLongRecord> buffer;

  private CouchbaseDocumentService docSrv;

  public RestoreService(int bufferMaxItems, long bufferMaxSize) {
    this.bufferMaxItems = bufferMaxItems;
    this.bufferMaxSize = bufferMaxSize;

    logger.info("Creating " + this);

    this.buffer = new ArrayList<CouchbaseLongRecord>();
    this.sizeInBuffer = 0;

    AuthInfo authInfo = new AuthInfo("testDefault", "");
    String[] nodes = {"172.17.0.2"};

    docSrv = new CouchbaseDocumentService(authInfo, nodes);
  }

  public void call(CouchbaseLongRecord doc) {
    buffer.add(doc);
    sizeInBuffer += doc.content().length;

    if ((buffer.size() == bufferMaxItems) || (sizeInBuffer >= bufferMaxSize)) {
      logger.info("Flushing buffer with items=" + buffer.size()
        + " having " + sizeInBuffer + "bytes.");

      int count = docSrv.saveDocumentsAsync(buffer);

      logger.info("Saved " + count + " documents.");

      buffer.clear();
      sizeInBuffer = 0;
    }
  }

  public void teardown() {
    int count = docSrv.saveDocumentsAsync(buffer);

    logger.info("Saved " + count + " documents.");

    buffer.clear();
    sizeInBuffer = 0;
  }
  @Override
  public String toString() {
    return "RestoreService [bufferMaxItems=" + bufferMaxItems
        + ", bufferMaxSize=" + bufferMaxSize + "]";
  }

}
