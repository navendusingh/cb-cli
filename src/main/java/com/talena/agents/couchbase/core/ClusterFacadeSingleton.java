package com.talena.agents.couchbase.core;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.env.DefaultCoreEnvironment;

public class ClusterFacadeSingleton {
  private static volatile ClusterFacade core = null;

  public static ClusterFacade core() {
    if (core == null) {
      synchronized (ClusterFacade.class) {
        if (core == null) {
          core = new CouchbaseCore(DefaultCoreEnvironment
            .builder()
            .dcpEnabled(true)
            .mutationTokensEnabled(true)
            .build());
        }
      }
    }

    return core;
  }
}
