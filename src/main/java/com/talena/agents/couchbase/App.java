package com.talena.agents.couchbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.talena.agents.couchbase.exception.UsageException;
import com.talena.agents.couchbase.service.CouchbaseDocumentService;
import com.talena.agents.couchbase.service.CouchbaseTestService;

public class App {
  private final static Logger logger = Logger.getLogger(App.class);

  public static void main(String[] args) {
    try {
      if (args.length < 2) {
        throw new UsageException("");
      }

      if (args[0].compareToIgnoreCase("testConnect") == 0) {
        if (args.length != 3) {
          throw new UsageException("testConnect");
        }

        AuthInfo authInfo = getAuthInfo(args[1]);
        String nodes[] = getNodes(args[2]);

        testConnect(authInfo, nodes);
      } else if (args[0].compareToIgnoreCase("getDoc") == 0) {
        if (args.length < 4) {
          throw new UsageException("getDoc");
        }

        AuthInfo authInfo = getAuthInfo(args[1]);
        String nodes[] = getNodes(args[2]);

        if (args.length == 4) {
          JsonDocument doc = getDocument(authInfo, nodes, args[3], false);

          if (doc == null) {
            System.out.println("Id: " + args[3] + ". Document not found.");
          } else {
            System.out.println(doc);
          }
        } else {
          List<String> ids = new ArrayList<String>();

          for (int i = 3; i < args.length; ++i) {
            ids.add(args[i]);
          }

          List<JsonDocument> docs = getDocuments(authInfo, nodes, ids, false);

          for (JsonDocument doc : docs) {
            System.out.println(doc);
          }
        }
      } else if (args[0].compareToIgnoreCase("saveDoc") == 0) {
        if (args.length != 3) {
          throw new UsageException("saveDoc");
        }

        AuthInfo authInfo = getAuthInfo(args[1]);
        String nodes[] = getNodes(args[2]);

        JsonObject obj = JsonObject.empty().put("id", "001");
        JsonDocument doc = JsonDocument.create("001", 2, obj);

        saveDocument(authInfo, nodes, doc, true);

        JsonDocument d = getDocument(authInfo, nodes, "001", false);
        System.out.println(d);
      }
    } catch (final UsageException e) {
      printUsage(e.toString());
    }
  }

  private static int saveDocument(
      final AuthInfo authInfo, final String[] nodes,
      final JsonDocument doc, boolean isAsync) {
    CouchbaseDocumentService docSrv = new CouchbaseDocumentService(
        authInfo, nodes);
    int result = 0;

    if (isAsync) {
      result = docSrv.saveDocumentAsync(doc);
    } else {
      result = docSrv.saveDocument(doc);
    }

    return result;
  }

  private static List<JsonDocument> getDocuments(
      final AuthInfo authInfo, final String[] nodes,
      final List<String> ids, boolean isAsync) {
    logger.info(authInfo);
    logger.info(nodes.length + " nodes.");

    CouchbaseDocumentService docSrv = new CouchbaseDocumentService(
        authInfo, nodes);

    List<JsonDocument> docs = null;

    if (isAsync) {
      docs = docSrv.getDocumentsAsync(ids);
    } else {
      docs = docSrv.getDocuments(ids);
    }

    return docs;
  }

  private static JsonDocument getDocument(
      final AuthInfo authInfo, final String[] nodes,
      final String id, boolean isAsync) {
    logger.info(authInfo);
    logger.info(nodes.length + " nodes.");

    CouchbaseDocumentService docSrv = new CouchbaseDocumentService(
        authInfo, nodes);

    JsonDocument doc = null;

    if (isAsync) {
      doc = docSrv.getDocumentAsync(id);
    } else {
      doc = docSrv.getDocument(id);
    }

    return doc;
  }

  private static void testConnect(
      final AuthInfo authInfo, final String[] nodes) {
    logger.info(authInfo);
    logger.info(nodes.length + " nodes.");

    CouchbaseTestService testSrv = new CouchbaseTestService(authInfo, nodes);
    logger.info("No of vBuckets is: " + testSrv.getVBucketsCount());
  }

  private static AuthInfo getAuthInfo(final String str)
      throws UsageException {
    String splits[] = str.split(":");
    if (splits.length > 2 ) {
      throw new UsageException("authInfo");
    }

    AuthInfo authInfo = new AuthInfo(splits[0], "");
    if (splits.length == 2) {
      authInfo.setPassword(splits[1]);
    }

    return authInfo;
  }

  private static String[] getNodes(final String str) {
    String splits[] = str.split(",");

    return splits;
  }

  private static void printUsage(final String message) {
    System.out.println("Usage: " + message);
  }
}
