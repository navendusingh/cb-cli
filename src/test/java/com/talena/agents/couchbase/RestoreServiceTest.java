package com.talena.agents.couchbase;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import com.talena.agents.couchbase.commons.CouchbaseLongRecord;
import com.talena.agents.couchbase.service.RestoreService;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class RestoreServiceTest extends TestCase {
  private RestoreService restoreSrv;

  public RestoreServiceTest(String testname) {
    super(testname);

    restoreSrv = new RestoreService(3, 15);
  }

  public static Test suite() {
    return new TestSuite(RestoreServiceTest.class);
  }

  public void testRestore() {
    try {
      DataInputStream inMutations = new DataInputStream(
          new FileInputStream(
              "/home/navendu/backups/thread1.backup.data.full.mut"));

      while (inMutations.available() > 0) {
        CouchbaseLongRecord doc = CouchbaseLongRecord.create();

        doc.readFields(inMutations);
        System.out.println(doc.toString());

        restoreSrv.call(doc);
      }

      restoreSrv.teardown();
      inMutations.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
