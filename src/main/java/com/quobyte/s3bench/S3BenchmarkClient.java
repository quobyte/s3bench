/*
 * Copyright 2016-2017 Quobyte Inc. All rights reserved.
 * Author: flangner@quobyte.com
 */
package com.quobyte.s3bench;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.quobyte.s3bench.Configuration.Mode;

public class S3BenchmarkClient {

  private static AWSCredentialsProvider credentialsProvider;
  private static Configuration configuration;
  private static int clients;
  private static int threads;

  private static abstract class Sampler extends Thread {

    private final boolean limitToSampleCount;
    private final long sampleSize;

    boolean done;
    long max;
    long min;
    long avg;
    long counter;
    double opS;
    double bpS;

    Sampler(long sampleSize, boolean limitToSampleCount) {
      this.sampleSize = sampleSize;
      this.limitToSampleCount = limitToSampleCount;
    }

    abstract void sampleRun(long sampleId) throws Throwable;

    @Override
    public void interrupt() {
      done = true;
      super.interrupt();
    }

    @Override
    public void run() {
      max = 0;
      min = Long.MAX_VALUE;
      long sumNanos = 0;
      counter = 0;
      while (!done) {
        try {
          long timeNanos = -System.nanoTime();
          sampleRun(counter);
          timeNanos += System.nanoTime();
          sumNanos += timeNanos;
          max = Math.max(max, timeNanos);
          min = Math.min(min, timeNanos);
          counter++;
          if (limitToSampleCount && counter == configuration.getSampleCount()) {
            break;
          }
        } catch (Throwable th) {
          if (!done) {
            System.err.println(th);
            th.printStackTrace();
            System.exit(1);
            return;
          }
        }
      }
      avg = sumNanos / counter;
      opS = (double) (1000l * 1000l * 1000l) / (double) avg;
      bpS = sampleSize * opS;
    }
  }

  private static class PutClient extends Sampler {

    private final AmazonS3 client;
    private final byte[] data;
    private final String bucket;
    private final ObjectMetadata metadata;
    private final String clientId;

    PutClient(String clientId, AmazonS3 client, String bucket, byte[] data, boolean limited) {
      super(data.length, limited);
      this.clientId = clientId;
      this.client = client;
      this.data = data;
      this.bucket = bucket;
      metadata = new ObjectMetadata();
      metadata.setContentLength(data.length);
    }

    @Override
    void sampleRun(long sampleId) throws Throwable {
      int sample = (int) (sampleId % configuration.getSampleCount());
      client.putObject(bucket, clientId + sample, new ByteArrayInputStream(data), metadata);
    }
  }

  private static class GetClient extends Sampler {
    private final AmazonS3 client;
    private final String clientId;
    private final String bucket;
    private final byte[] data;

    GetClient(String clientId, AmazonS3 client, String bucket, byte[] data, boolean limited) {
      super(data.length, limited);
      this.clientId = clientId;
      this.client = client;
      this.bucket = bucket;
      this.data = data;
    }

    @Override
    void sampleRun(long sampleId) throws Throwable {
      int sample = (int) (sampleId % configuration.getSampleCount());
      S3Object object = client.getObject(bucket, clientId + sample);
      assert (object.getObjectMetadata().getContentLength() == data.length)
          : object.getObjectMetadata().getContentLength() + " != " + data.length + " for "
            + clientId + sample;

      long dataRead = 0;
      try (S3ObjectInputStream in = object.getObjectContent()) {
        int chunkRead = 0;
        while ((chunkRead = in.read(data)) >= 0) {
          dataRead += chunkRead;
        }
        assert dataRead == data.length;

      } catch (IOException e) {
        System.err.println(e);
        e.printStackTrace();
        System.exit(1);
        return;
      }
    }
  }

  private static void singleStreamPerformanceTest(
      String host,
      String bucket,
      Mode mode) {
    AmazonS3 client = client(host, mode);
    byte[] data;
    if (configuration.benchmarkOps()) {
      System.out.println("## single stream latency");
      System.out.println("Object size: " + formattedObjectSize(configuration.getOpsObjectSize()));
      System.out.println();
      data = new byte[configuration.getOpsObjectSize()];
      ThreadLocalRandom.current().nextBytes(data);
      System.out.println("method, min, max, avg, throughput (based on avg), ops/s (based on avg),"
          + " operation count");
      singleStreamWritePerformance(client, bucket, data);
      if (configuration.benchmarkGet()) {
        singleStreamReadPerformance(client, bucket, data);
      }
      System.out.println();
    }

    if (configuration.benchmarkTp()) {
      System.out.println("## single stream throughput");
      System.out.println("Object size: " + formattedObjectSize(configuration.getTpObjectSize()));
      System.out.println();
      data = new byte[configuration.getTpObjectSize()];
      ThreadLocalRandom.current().nextBytes(data);
      System.out.println("method, min, max, avg, throughput (based on avg), ops/s (based on avg),"
          + " operation count");
      singleStreamWritePerformance(client, bucket, data);
      if (configuration.benchmarkGet()) {
        singleStreamReadPerformance(client, bucket, data);
      }
      System.out.println();
    }

    client.shutdown();
  }

  private static void singleStreamWritePerformance(AmazonS3 client, String bucket, byte[] data) {
    PutClient putClient = new PutClient("client0.0.", client, bucket, data, true);
    putClient.run();
    System.out.println("put, "
        + formattedNsToMs(putClient.min) + ", "
        + formattedNsToMs(putClient.max) + ", "
        + formattedNsToMs(putClient.avg) + ", "
        + formattedBps(putClient.bpS) + ", "
        + formatOps(putClient.opS) + ", "
        + formattedOpCount(putClient.counter));
  }

  private static void singleStreamReadPerformance(AmazonS3 client, String bucket, byte[] data) {
    GetClient getClient = new GetClient("client0.0.", client, bucket, data, true);
    getClient.run();
    System.out.println("get, "
        + formattedNsToMs(getClient.min) + ", "
        + formattedNsToMs(getClient.max) + ", "
        + formattedNsToMs(getClient.avg) + ", "
        + formattedBps(getClient.bpS) + ", "
        + formatOps(getClient.opS) + ", "
        + formattedOpCount(getClient.counter));
  }

  private static void multiStreamPerformanceTest(
      String host,
      String bucket,
      Mode mode) throws InterruptedException {
    byte[] data;

    if (configuration.benchmarkOps()) {
      System.out.println("## multi-stream latency");
      System.out.println("Object size: " + formattedObjectSize(configuration.getOpsObjectSize()));
      clients = configuration.getMultiOpsClients();
      threads = configuration.getMultiOpsThreads();
      System.out.println("Number of clients: " + clients);
      System.out.println("Number of threads per client: " + threads);
      System.out.println();
      data = new byte[configuration.getOpsObjectSize()];
      ThreadLocalRandom.current().nextBytes(data);
      System.out.println("method, min, max, avg, aggregated throughput (based on avg),"
          + " aggregated ops/s (based on avg),"
          + " aggregated operation count");
      boolean objectLayoutComplete = multiStreamWritePerformance(host, bucket, mode, data);
      if (configuration.benchmarkGet()) {
        if (objectLayoutComplete) {
          multiStreamReadPerformance(host, bucket, mode, data);
        } else {
          System.out.println("INFO: multi-stream read was skipped."
              + " Not all objects could have been laid out during the write phase.");
          System.out.println("INFO: Extend the runtime or decrease the target sample count.");
        }
      }
      System.out.println();
    }

    if (configuration.benchmarkTp()) {
      System.out.println("## multi-stream throughput");
      System.out.println("Object size: " + formattedObjectSize(configuration.getTpObjectSize()));
      clients = configuration.getMultiTpClients();
      threads = configuration.getMultiTpThreads();
      System.out.println("Number of clients: " + clients);
      System.out.println("Number of threads per client: " + threads);
      System.out.println();
      data = new byte[configuration.getTpObjectSize()];
      ThreadLocalRandom.current().nextBytes(data);
      System.out.println("method, min, max, avg, aggregated throughput (based on avg),"
          + " aggregated ops/s (based on avg),"
          + " aggregated operation count");
      boolean objectLayoutComplete = multiStreamWritePerformance(host, bucket, mode, data);
      if (configuration.benchmarkGet()) {
        if (objectLayoutComplete) {
          multiStreamReadPerformance(host, bucket, mode, data);
        } else {
          System.out.println("INFO: multi-stream read was skipped."
              + " Not all objects could have been laid out during the write phase.");
          System.out.println("INFO: Extend the runtime or decrease the target sample count.");
        }
      }
      System.out.println();
    }
  }

  /**
   * @return true if all objects have been laid out properly.
   */
  private static boolean multiStreamWritePerformance(
      String host,
      String bucket,
      Mode mode,
      byte[] data) throws InterruptedException {
    final int totalThreads = clients * threads;
    AmazonS3[] amazonClients = new AmazonS3[clients];
    PutClient[] putClients = new PutClient[totalThreads];
    int i = 0;
    for (int c = 0; c < clients; c++) {
      amazonClients[c] = client(host, mode);
      for (int t = 0; t < threads; t++) {
        putClients[i++] = new PutClient(
            "client" + c + "." + t + ".",
            amazonClients[c],
            bucket,
            data,
            false);
      }
    }
    for (PutClient putClient : putClients) {
      putClient.start();
    }
    Thread.sleep(configuration.getMultiStreamRuntimeS() * 1000);
    for (PutClient putClient : putClients) {
      putClient.interrupt();
    }
    long minOfMins = Long.MAX_VALUE;
    long maxOfMaxs = 0;
    long sumOfAvgs = 0;
    long sumOfBps = 0;
    double sumOfOps = 0;
    long sumOfCounter = 0;
    boolean objectLayoutComplete = true;
    for (PutClient putClient : putClients) {
      putClient.join();
      minOfMins = Math.min(minOfMins, putClient.min);
      maxOfMaxs = Math.max(maxOfMaxs, putClient.max);
      sumOfAvgs += putClient.avg;
      sumOfBps += putClient.bpS;
      sumOfOps += putClient.opS;
      sumOfCounter += putClient.counter;
      objectLayoutComplete &= putClient.counter >= configuration.getSampleCount();
    }
    for (AmazonS3 amazonClient : amazonClients) {
      amazonClient.shutdown();
    }
    System.out.println("PUT, "
        + formattedNsToMs(minOfMins) + ", "
        + formattedNsToMs(maxOfMaxs) + ", "
        + formattedNsToMs(sumOfAvgs / totalThreads) + ", "
        + formattedBps(sumOfBps) + ", "
        + formatOps(sumOfOps) + ", "
        + formattedOpCount(sumOfCounter));
    return objectLayoutComplete;
  }

  private static void multiStreamReadPerformance(
      String host,
      String bucket,
      Mode mode,
      byte[] data) throws InterruptedException {
    final int totalThreads = clients * threads;
    AmazonS3[] amazonClients = new AmazonS3[clients];
    GetClient[] getClients = new GetClient[totalThreads];
    int i = 0;
    for (int c = 0; c < clients; c++) {
      amazonClients[c] = client(host, mode);
      for (int t = 0; t < threads; t++) {
        getClients[i++] = new GetClient(
            "client" + c + "." + t + ".",
            amazonClients[c],
            bucket,
            data,
            false);
      }
    }
    for (GetClient getClient : getClients) {
      getClient.start();
    }
    Thread.sleep(configuration.getMultiStreamRuntimeS() * 1000);
    for (GetClient getClient : getClients) {
      getClient.interrupt();
    }
    long minOfMins = Long.MAX_VALUE;
    long maxOfMaxs = 0;
    long sumOfAvgs = 0;
    long sumOfBps = 0;
    double sumOfOps = 0;
    long sumOfCounter = 0;
    for (GetClient getClient : getClients) {
      getClient.join();
      minOfMins = Math.min(minOfMins, getClient.min);
      maxOfMaxs = Math.max(maxOfMaxs, getClient.max);
      sumOfAvgs += getClient.avg;
      sumOfBps += getClient.bpS;
      sumOfOps += getClient.opS;
      sumOfCounter += getClient.counter;
    }
    for (AmazonS3 amazonClient : amazonClients) {
      amazonClient.shutdown();
    }
    System.out.println("GET, "
        + formattedNsToMs(minOfMins) + ", "
        + formattedNsToMs(maxOfMaxs) + ", "
        + formattedNsToMs(sumOfAvgs / totalThreads) + ", "
        + formattedBps(sumOfBps) + ", "
        + formatOps(sumOfOps) + ", "
        + formattedOpCount(sumOfCounter));
  }

  public static void main(String[] args) throws InterruptedException {
    if (args == null || args.length < 4) {
      printUsage();
      System.exit(1);
      return;
    }
    try {
      String host = args[0];
      String bucket = args[1];
      String awsAccessKeyId = args[2];
      String awsSecretKey = args[3];
      String configurationPath = args.length > 4 ? args[4] : null;
      AWSCredentials credentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey);
      credentialsProvider = new AWSCredentialsProvider() {
        @Override
        public void refresh() {}

        @Override
        public AWSCredentials getCredentials() {
          return credentials;
        }
      };
      configuration = new Configuration(configurationPath);

      System.out.println("S3 Service Benchmark (powered by Amazon SDK 1.11.186)");
      System.out.println("Path-style bucket addressing: " + configuration.pathStyle());
      System.out.println("HTTPS: " + configuration.useSsl());
      System.out.println("Client-side MD5 verification: "
          + configuration.isClientMd5VerificationEnabled());
      if (configuration.benchmarkUnsigned()
          || configuration.benchmarkPresigned()
          || configuration.benchmarkChunked()) {
        AmazonS3 client = client(host, Mode.CHUNKED);
        if (!client.doesBucketExist(bucket)) {
          if (configuration.createBucket()) {
            System.out.println("PRE: Creating bucket '" + bucket + "'.");
            client.createBucket(bucket);
            System.out.println("PRE: Bucket creation successful.");
          } else {
            System.err.println("Bucket '" + bucket + "' does not exist.");
            System.exit(1);
            return;
          }
        }
        client.shutdown();
      }
      if (!configuration.isClientMd5VerificationEnabled()) {
        System.setProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY, "true");
        System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");
      }
      System.out.println("Number of samples: " + configuration.getSampleCount());
      System.out.println("Throughput runtime: " + configuration.getMultiStreamRuntimeS() + " s");
      System.out.println();
      System.out.println();

      if (configuration.benchmarkUnsigned()) {
        System.out.println("# unsigned payload benchmark");
        if (configuration.benchmarkSingleStream()) {
          singleStreamPerformanceTest(host, bucket, Mode.UNSIGNED);
          System.out.println();
        }
        if (configuration.benchmarkMultiStream()) {
          multiStreamPerformanceTest(host, bucket, Mode.UNSIGNED);
        }
        System.out.println();
        System.out.println();
      }

      if (configuration.benchmarkPresigned()) {
        System.out.println("# SHA256 signed payload benchmark");
        if (configuration.benchmarkSingleStream()) {
          singleStreamPerformanceTest(host, bucket, Mode.PRESIGNED);
          System.out.println();
        }
        if (configuration.benchmarkMultiStream()) {
          multiStreamPerformanceTest(host, bucket, Mode.PRESIGNED);
        }
        System.out.println();
        System.out.println();
      }

      if (configuration.benchmarkChunked()) {
        System.out.println("# Signature V4 chunked encoding benchmark");
        if (configuration.benchmarkSingleStream()) {
          singleStreamPerformanceTest(host, bucket, Mode.CHUNKED);
          System.out.println();
        }
        if (configuration.benchmarkMultiStream()) {
          multiStreamPerformanceTest(host, bucket, Mode.CHUNKED);
        }
      }

      if (configuration.cleanAfterRun()) {
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("POST: Cleaning the bucket.");
        AmazonS3 client = client(host, Mode.CHUNKED);
        long time = -System.nanoTime();
        ObjectListing objects;
        long numDeleted = 0;
        do {
          objects = client.listObjects(bucket);
          DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
          String[] keys = new String[objects.getObjectSummaries().size()];
          int i = 0;
          for (S3ObjectSummary object : objects.getObjectSummaries()) {
            keys[i++] = object.getKey();
          }
          numDeleted += client.deleteObjects(request.withKeys(keys)).getDeletedObjects().size();
        } while (!objects.getObjectSummaries().isEmpty());
        time += System.nanoTime();
        System.out.println("POST: Cleaned " + numDeleted + " objects in " + formattedNsToMs(time));
        client.shutdown();
      }

      System.exit(0);
    } catch (Throwable e) {
      System.err.println(e);
      e.printStackTrace();
      printUsage();
      System.exit(1);
    }
  }

  private static void printUsage() {
    System.out.println("s3Bench.sh <host> <bucket> <accessKeyId> <secret> [config.properties]");
  }

  private static AmazonS3 client(String host, Mode mode) {
    AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();
    clientBuilder.setCredentials(credentialsProvider);
    ClientConfiguration clientConfiguration = new ClientConfiguration()
        .withProtocol(configuration.useSsl()
            ? Protocol.HTTPS
            : Protocol.HTTP)
        .withTcpKeepAlive(true)
        .withSocketTimeout(2 * 60 * 1000);
    switch (mode) {
      case UNSIGNED:
        clientConfiguration.setSignerOverride("S3SignerType");
      case PRESIGNED:
        clientBuilder.setChunkedEncodingDisabled(true);
        break;
      case CHUNKED:
      default:
        break;
    }
    clientConfiguration.setCacheResponseMetadata(false);
    clientBuilder.setClientConfiguration(clientConfiguration);
    if (configuration.pathStyle()) {
      clientBuilder.setPathStyleAccessEnabled(true);
    }
    clientBuilder.setEndpointConfiguration(new EndpointConfiguration(host, null));
    return clientBuilder.build();
  }

  private static String formattedOpCount(double operations) {
    if (operations > 1000.0) {
      // K
      double koperations = operations / 1000.0;
      if (koperations > 1000.0) {
        // M
        return String.format("%.2f M", koperations / 1000.0);
      } else {
        return String.format("%.2f K", koperations);
      }
    } else {
      return String.format("%.2f ", operations);
    }
  }

  private static String formatOps(double ops) {
    return formattedOpCount(ops) + "/s";
  }

  private static String formattedBps(double bps) {
    if (bps > 1000.0) {
      // K
      double kbps = bps / 1000.0;
      if (kbps > 1000.0) {
        // M
        double mbps = kbps / 1000.0;
        if (mbps > 1000.0) {
          // G
          return String.format("%.2f Mb/s", mbps / 1000.0);
        } else {
          return String.format("%.2f Mb/s", mbps);
        }
      } else {
        return String.format("%.2f Kb/s", kbps);
      }
    } else {
      return String.format("%.2f b/s", bps);
    }
  }

  private static String formattedNsToMs(long ns) {
    double ms = (double) ns / (1000.0 * 1000.0);
    return String.format("%.2f ms", ms);
  }

  private static String formattedObjectSize(int bytes) {
    if (bytes > 1024) {
      // KiB
      double kbytes = bytes / 1024.0;
      if (kbytes > 1024.0) {
        // MiB
        return String.format("%.2f MiB", kbytes / 1024.0);
      } else {
        return String.format("%.2f KiB", kbytes);
      }
    } else {
      return String.format("%d B", bytes);
    }
  }
}
