package com.talena.agents.couchbase.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.talena.agents.couchbase.AuthInfo;
import com.talena.agents.couchbase.commons.CouchbaseLongRecord;

import rx.Observable;
import rx.functions.Func1;

public class CouchbaseDocumentService {
  private final static Logger logger = Logger.getLogger(
      CouchbaseDocumentService.class);

  private AuthInfo bucketAuthInfo;
  private String[] nodes;

  public CouchbaseDocumentService(
      final AuthInfo bucketAuthInfo, final String[] nodes) {
    this.bucketAuthInfo = new AuthInfo(bucketAuthInfo);
    this.nodes = nodes.clone();
  }

  public JsonDocument getJsonDocument(final String id) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());
    JsonDocument doc = bucket.get(id);

    cbCluster.disconnect();

    return doc;
  }

  public List<JsonDocument> getJsonDocuments(final List<String> ids) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());
    List<JsonDocument> docs = new ArrayList<JsonDocument>();

    for (String id: ids) {
      JsonDocument doc = bucket.get(id);

      if (doc != null) {
        docs.add(doc);
      }
    }

    cbCluster.disconnect();

    return docs;
  }

  public int saveJsonDocument(JsonDocument doc) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    JsonDocument d = bucket.upsert(doc);
    logger.info("Created: " + d);

    cbCluster.disconnect();

    return 1;
  }

  public JsonDocument getJsonDocumentAsync(final String id) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    List<JsonDocument> docs = Observable
        .just(id)
        .flatMap(new Func1<String, Observable<JsonDocument>>() {
          public Observable<JsonDocument> call(String id) {
            return bucket.async().get(id);
          }
        })
        .toList()
        .toBlocking()
        .single();

    cbCluster.disconnect();

    JsonDocument doc = null;

    if (!docs.isEmpty()) {
      doc = docs.get(0);
    }

    return doc;
  }

  public List<JsonDocument> getJsonDocumentsAsync(final List<String> ids) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    List<JsonDocument> docs = Observable
        .from(ids)
        .flatMap(new Func1<String, Observable<JsonDocument>>() {
          public Observable<JsonDocument> call(String id) {
            return bucket.async().get(id);
          }
        })
        .toList()
        .toBlocking()
        .single();

    cbCluster.disconnect();

    return docs;
  }

  public int saveJsonDocumentAsync(JsonDocument doc) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    List<JsonDocument> docsCreated = Observable
      .just(doc)
      .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
        public Observable<JsonDocument> call(JsonDocument doc) {
          return bucket.async().upsert(doc);
        }
      })
      .toList()
      .toBlocking()
      .single();

    cbCluster.disconnect();

    logger.info("Created " + docsCreated.size() + " documents.");

    return docsCreated.size();
  }

  public int saveJsonDocumentsAsync(List<JsonDocument> docs) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    List<JsonDocument> docsCreated = Observable
      .from(docs)
      .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
        public Observable<JsonDocument> call(JsonDocument doc) {
          return bucket.async().upsert(doc);
        }
      })
      .toList()
      .toBlocking()
      .single();

    cbCluster.disconnect();

    logger.info("Created " + docsCreated.size() + " documents.");

    return docsCreated.size();
  }

  public <T extends Document> int saveDocumentAsync(final T doc) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    List<T> docsCreated = Observable
      .just(doc)
      .flatMap(new Func1<T, Observable<T>>() {
        public Observable<T> call(T doc) {
          return bucket.async().upsert(doc);
        }
      })
      .toList()
      .toBlocking()
      .single();

    cbCluster.disconnect();

    logger.info("Created " + docsCreated.size() + " documents.");

    return docsCreated.size();
  }

  public <T extends Document> int saveDocumentsAsync(final List<T> doc) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    List<T> docsCreated = Observable
      .from(doc)
      .flatMap(new Func1<T, Observable<T>>() {
        public Observable<T> call(T doc) {
          return bucket.async().upsert(doc);
        }
      })
      .toList()
      .toBlocking()
      .single();

    cbCluster.disconnect();

    logger.info("Created " + docsCreated.size() + " documents.");

    return docsCreated.size();
  }

  public boolean saveDocumentAsync(final String key, final ByteBuf content) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    bucket
      .core()
      .send(new UpsertRequest(key, content, bucket.name()))
      .toBlocking()
      .single();

    cbCluster.disconnect();

    return true;
  }

  public int saveDocumentsAsync(Iterable<CouchbaseLongRecord> docs) {
    Cluster cbCluster = CouchbaseCluster.create(Arrays.asList(nodes));
    final Bucket bucket = cbCluster.openBucket(
        bucketAuthInfo.getName(), bucketAuthInfo.getPassword());

    int count = Observable
      .from(docs)
      .flatMap(new Func1<CouchbaseLongRecord, Observable<CouchbaseResponse>>(){
        public Observable<CouchbaseResponse> call(CouchbaseLongRecord doc) {
          return bucket.core().send(new UpsertRequest(
              doc.key(), Unpooled.copiedBuffer(doc.content()), bucket.name()));
        }})
      .toList()
      .count()
      .toBlocking()
      .single();

    cbCluster.disconnect();

    return count;
  }
}
