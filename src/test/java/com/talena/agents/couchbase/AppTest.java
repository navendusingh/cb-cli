package com.talena.agents.couchbase;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import com.talena.agents.couchbase.service.CouchbaseDocumentService;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AppTest extends TestCase {
  private CouchbaseDocumentService docSrv;

  public AppTest(String testName) {
    super(testName);

    AuthInfo authInfo = new AuthInfo("default", "");
    String[] nodes = {"172.17.0.2"};

    docSrv = new CouchbaseDocumentService(authInfo, nodes);
  }

  public static Test suite() {
    return new TestSuite(AppTest.class);
  }

  public void testSaveDocumentAsync() {
    String id = "001";

    JsonObject obj = JsonObject.empty().put("id", id);
    JsonDocument doc = JsonDocument.create("001", obj);
    int count = docSrv.saveDocumentAsync(doc);

    assertTrue(count == 1);

    JsonDocument docFetched = docSrv.getJsonDocument(id);

    assertTrue(docFetched.content().toString().compareTo(
        doc.content().toString()) == 0);
  }

  public void testSaveDocumentsAsync() {
    List<JsonDocument> docs = new ArrayList<JsonDocument>();

    JsonObject obj = JsonObject.empty().put("id", "002");
    JsonDocument doc = JsonDocument.create("002", obj);
    docs.add(doc);

    obj = JsonObject.empty().put("id", "003");
    doc = JsonDocument.create("003", obj);
    docs.add(doc);

    int count = docSrv.saveDocumentsAsync(docs);

    assertTrue(count == 2);
  }

  public void testSaveJsonDocumentUsingUpsertRequest() {
    String id = "004";

    JsonTranscoder jt = new JsonTranscoder();

    JsonObject obj = JsonObject.empty().put("id", id);
    JsonDocument doc = JsonDocument.create("004", obj);
    boolean result = false;
    try {
      result = docSrv.saveDocumentAsync(id, jt.jsonObjectToByteBuf(obj));
    } catch (Exception e) {
      e.printStackTrace();
    }

    assertTrue(result == true);

    JsonDocument docFetched = docSrv.getJsonDocument(id);

    System.out.println(docFetched.toString());

    assertTrue(docFetched.content().toString().compareTo(
        doc.content().toString()) == 0);
  }

  public void testSaveDocumentUsingUpsertRequest() {
    String id = "005";

    boolean result = false;
    String str = "Hello World";
    try {
      result = docSrv.saveDocumentAsync(
          id, Unpooled.copiedBuffer(str.getBytes()));
    } catch (Exception e) {
      e.printStackTrace();
    }

    assertTrue(result == true);
  }
}
