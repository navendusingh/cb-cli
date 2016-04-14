package com.talena.agents.couchbase.service;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;

public class CouchbaseClusterService {
  private final static Logger logger = Logger.getLogger(
      CouchbaseClusterService.class);

  private String[] nodes;

  public CouchbaseClusterService(final String[] nodes) {
    this.nodes = nodes.clone();
  }

  public List<BucketSettings> getBuckets(final String userName, final String password) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    ClusterManager mgrCluster = cbCluster.clusterManager(userName, password);

    List<BucketSettings> buckets = mgrCluster.getBuckets();

    cbCluster.disconnect();

    logger.info("Found " + buckets.size() + " bucket(s).");

    return buckets;
  }
}
