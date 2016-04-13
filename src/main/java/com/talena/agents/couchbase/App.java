package com.talena.agents.couchbase;

import com.talena.agents.couchbase.exception.UsageException;
import com.talena.agents.couchbase.service.CouchbaseTestService;

public class App {
  public static void main(String[] args) {
    try {
      if (args.length < 2) {
        throw new UsageException("");
      }

      if (args[0].compareToIgnoreCase("testConnect") == 0) {
        if (args.length != 3) {
          throw new UsageException("testConnect");
        }

        BucketAuthInfo bucket = getBucketInfo(args[1]);
        String nodes[] = getNodes(args[2]);

        testConnect(bucket, nodes);
      }
    } catch (final UsageException e) {
      printUsage(e.toString());
    }
  }

  private static void testConnect(final BucketAuthInfo bucket, final String[] nodes) {
    System.out.println(bucket);
    System.out.println(nodes.length + " nodes.");

    CouchbaseTestService testSrv = new CouchbaseTestService(bucket, nodes);
    System.out.println("No of vBuckets is: " + testSrv.getVBucketsCount());
  }

  private static BucketAuthInfo getBucketInfo(final String str)
      throws UsageException {
    String splits[] = str.split(":");
    if (splits.length > 2 ) {
      throw new UsageException("bucketInfo");
    }

    BucketAuthInfo bucket = new BucketAuthInfo(splits[0], "");
    if (splits.length == 2) {
      bucket.setPassword(splits[1]);
    }

    return bucket;
  }

  private static String[] getNodes(final String str) {
    String splits[] = str.split(",");

    return splits;
  }

  private static void printUsage(final String message) {
    System.out.println("Usage: " + message);
  }
}
