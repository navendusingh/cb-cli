package com.talena.agents.couchbase.benchmark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.talena.agents.couchbase.core.CouchbaseFacade;
import com.talena.agents.couchbase.core.PartitionSpec;

public class BenchmarkBackup {
  private static void printUsage() {
    System.out.println(
        "Usage: <bucket> <backupPaths> <Full/Incremental> <ipNodes> <threads>");
  }

  public static void main(String[] args) {
    if (args.length < 5) {
      printUsage();

      return;
    }

    String bucket = args[0];
    String backupPaths[] = args[1].split(":");
    String backupType = args[2];
    String nodes[] = args[3].split(",");
    int threadsCount = 1;
    boolean isFullBackup = true;

    try {
      threadsCount = Integer.parseInt(args[4]);
    } catch (NumberFormatException e) {
      printUsage();

      return;
    }

    if (backupType.compareToIgnoreCase("Full") == 0) {
      isFullBackup = true;
    } else if (backupType.compareToIgnoreCase("Incremental") == 0) {
      isFullBackup = false;
    } else {
      printUsage();

      return;
    }

    BenchmarkBackup app = new BenchmarkBackup();
    app.Run(bucket, nodes, backupPaths, isFullBackup, threadsCount);

    System.out.print("Backup Type    : ");
    if (backupType.compareToIgnoreCase("Full") == 0) {
      System.out.println("Full");
    } else if (backupType.compareToIgnoreCase("Incremental") == 0) {
      System.out.println("Incremental");
    }

    System.out.println("Bucket         : " + bucket);

    System.out.println("Backup Path(s) : ");
    for (String backupPath : backupPaths) {
      System.out.println("\t" + backupPath);
    }

    System.out.println("Node(s)        : ");
    for (String node : nodes) {
      System.out.println("\t" + node);
    }

    System.out.println("Thread count   : " + threadsCount);
  }

  public void Run(String bucket, String[] nodes, String[] backupPaths,
      boolean isFullBackup, int threadsCount) {
    CouchbaseFacade cbFacade = new CouchbaseFacade(nodes, bucket, "");
    cbFacade.openBucket();

    Map<Short, Long> highSeqNos = new HashMap<Short, Long>();
    short changedPartitions[] = new short[1024];
    int changedPartitionsCount = 0;

    List<String> clusterNodes = cbFacade.getClusterNodes();
    cbFacade.closeBucket();

    for (String clusterNode : clusterNodes) {
      String[] arr = new String[1];

      System.out.println("Fetching high sequence number for node: "
        + clusterNode);

      arr[0] = clusterNode;
      cbFacade = new CouchbaseFacade(arr, bucket, "");
      cbFacade.openBucket();

      Map<Short, Long> seqNos = cbFacade.currentHighSeqnos();
      int count = 0;
      for (Map.Entry<Short, Long> seqNo : seqNos.entrySet()) {
        if (seqNo.getValue() != 0) {
          highSeqNos.put(seqNo.getKey(), seqNo.getValue());
          ++count;
        }
      }
      System.out.println("Added " + count + " entries to highSeqNos.");
      cbFacade.closeBucket();
    }

    System.out.println("vBuckets high sequence numbers are ...");

    for (Map.Entry<Short, Long> highSeqNo : highSeqNos.entrySet()) {
      if (highSeqNo.getValue() != 0) {
        System.out.println(highSeqNo.getKey() + " " + highSeqNo.getValue());
        changedPartitions[changedPartitionsCount] = highSeqNo.getKey().shortValue();
        ++changedPartitionsCount;
      }
    }

    Map<Short, PartitionSpec> specs[] =
        (Map<Short, PartitionSpec>[])new Map<?, ?>[threadsCount];
    for (int i = 0; i < threadsCount; ++i) {
      specs[i] = new HashMap<Short, PartitionSpec>();
    }
    for (int i = 0; i < changedPartitionsCount; ++i) {
      short idChanged = changedPartitions[i];
      long endSeqNo = highSeqNos.get(idChanged);
      long startSeqNo = 0;

      if (endSeqNo > startSeqNo) {
        PartitionSpec partSpec = new PartitionSpec(
            idChanged, 0, startSeqNo, endSeqNo, 0, 0);
        System.out.println("For partition " + idChanged
            + " range is " + startSeqNo + " to " + endSeqNo);
  
        int specsIndex = i % threadsCount;

        specs[specsIndex].put(idChanged, partSpec);
      }
    }

    for (int i = 3; i >= 1; --i) {
      System.out.print("\rStarting in " + i + " seconds.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    System.out.println("");

    BackupThread threads[] = new BackupThread[threadsCount];

    for (int i = 0; i < threadsCount; ++i) {
      threads[i] = new BackupThread(
          "Thread " + (i + 1),
          bucket, "", nodes, backupPaths[i % backupPaths.length]
          + "thread" + (i + 1) + ".", specs[i]);
      System.out.println("Created Thread " + (i + 1));
    }

    for (int i = 0; i < threadsCount; ++i) {
      threads[i].start();
    }

    try {
      for (BackupThread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    System.out.println("All mutations received.");

    for (int i = 0; i < threadsCount; ++i) {
      long startTime = threads[i].getStartTime();
      long endTime = threads[i].getEndTime();
      long timeTaken = endTime - startTime;

      System.out.println("Thread " + (i + 1) + "[Mutations="
          + threads[i].getMutationsReceived()
          + ", Bytes=" + threads[i].getBytesReceived()
          + ", Start Time=" + startTime
          + ", End Time=" + endTime
          + ", Time Taken=" + timeTaken + "ms]");
    }
  }
}
