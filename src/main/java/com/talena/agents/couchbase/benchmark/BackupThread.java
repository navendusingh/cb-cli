package com.talena.agents.couchbase.benchmark;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.talena.agents.couchbase.commons.CouchbaseLongRecord;
import com.talena.agents.couchbase.core.ClosedPartition;
import com.talena.agents.couchbase.core.CouchbaseFacade;
import com.talena.agents.couchbase.core.DCPEndpoint;
import com.talena.agents.couchbase.core.OpenPartition;
import com.talena.agents.couchbase.core.PartitionSpec;
import com.talena.agents.couchbase.core.PartitionStats;

import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.MathObservable;

public class BackupThread extends Thread {
  private String bucketName;
  private String password;
  private String[] nodes;
  private String backupPath;
  private Map<Short, PartitionSpec> specs;

  private long bytesCount = 0;
  private long startTime = 0;
  private long endTime = 0;
  private FileChannel fcOut = null;
  private AtomicLong mutationCount;

  private DataOutputStream outMutations;

  private long flushCounter = 0;
  private final long flushLimit = (10L * 20L * 1024L * 1024L);

  public long getMutationsReceived() {
    return mutationCount.get();
  }

  public long getBytesReceived() {
    return bytesCount;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public BackupThread(
      final String threadName,
      final String bucketName,
      final String password,
      final String[] nodes,
      final String backupPath,
      final Map<Short, PartitionSpec> specs) {
    super(threadName);

    this.bucketName = bucketName;
    this.password = password;
    this.nodes = nodes.clone();
    this.backupPath = backupPath;
    this.specs = new HashMap<Short, PartitionSpec>(specs);
  }

  public void run() {
    System.out.println("[" + this.getName() + "] Starting.");

    CouchbaseFacade cbFacade = new CouchbaseFacade(nodes, bucketName, password);
    cbFacade.openBucket();

    File backupFile = new File(backupPath + "backup.data.full");

    try {
      outMutations = new DataOutputStream(
          new FileOutputStream(backupFile.getAbsolutePath() + ".mut"));

      fcOut = new FileOutputStream(backupFile).getChannel();
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    }

    DCPEndpoint dcp = cbFacade.dcpEndpoint();
    dcp.openConnection(this.getName(), new HashMap<String, String>());

    mutationCount = new AtomicLong(0);

    startTime = System.currentTimeMillis();

    dcp.streamPartitions(specs)
    .toBlocking()
    .forEach(new Action1<DCPRequest>() {
      public void call(DCPRequest dcpRequest) {
        handleDCPRequest(dcpRequest);
      }
    });

    dcp.closedPartitions()
    .toBlocking()
    .forEach(new Action1<ClosedPartition>() {
      public void call(ClosedPartition p) {
      }
    });

    dcp.openPartitions()
    .toBlocking()
    .forEach(new Action1<OpenPartition>() {
      public void call(OpenPartition p) {
      }
    });

    endTime = System.currentTimeMillis();

    long mCount = MathObservable.sumLong(dcp
        .stats()
        .map(new Func1<PartitionStats, Long>() {
          public Long call(PartitionStats s) {
            return s.stats().get(PartitionStats.Stat.NUM_MUTATIONS);
          }
        }))
        .single()
        .toBlocking()
        .first();

    try {
      if (fcOut != null) {
        fcOut.force(true);
        fcOut.close();

        outMutations.flush();
        outMutations.close();
      }
    } catch (Exception e) {
      System.out.println(e);
    }

    System.out.println("[" + this.getName()
        + "] Ending. Written " + mCount + " mutation(s) to "
        + backupPath);
  }

  private void handleDCPRequest(final DCPRequest req) {
    if (req instanceof MutationMessage) {
      MutationMessage msg = (MutationMessage) req;
      handleMutation(msg, msg.partition());
    } else if (req instanceof RemoveMessage) {
      RemoveMessage msg = (RemoveMessage) req;
      handleRemove(msg, msg.partition());
    }
  }

  private void handleMutation(final MutationMessage msg, short partition) {
    mutationCount.incrementAndGet();

    try {
      msg.content().getBytes(0, fcOut, msg.content().capacity());
      flushCounter += msg.content().capacity();

      CouchbaseLongRecord lr = CouchbaseLongRecord.create(msg, partition);
      lr.write(outMutations);

      if (flushCounter >= flushLimit) {
        outMutations.flush();

        fcOut.force(true);
        flushCounter = 0;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.print("\r[" + this.getName()
        + "] Written mutation # " + mutationCount.get());

    bytesCount += msg.content().capacity();
    msg.content().release();
    msg.connection().consumed(msg);
  }

  private void handleRemove(final RemoveMessage msg, short partition) {
  }
}
