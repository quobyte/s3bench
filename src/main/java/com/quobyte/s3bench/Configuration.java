/*
 * Copyright 2017 Quobyte Inc. All rights reserved.
 * Author: flangner@quobyte.com
 */
package com.quobyte.s3bench;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class Configuration {

  static enum Mode {
    CHUNKED("benchmark_chunked"),
    PRESIGNED("benchmark_presigned"),
    UNSIGNED("benchmark_unsigned");

    private final String propertiesKey;

    private Mode(String propertiesKey) {
      this.propertiesKey = propertiesKey;
    }
  }

  private static final String DEFAULT_URI = "/com/quobyte/s3bench/default.properties";

  /**
   * Fallback, if default.properties could not be loaded.
   */
  private static final boolean DEFAULT_CLIENT_MD5_VERIFICATION = false;
  private static final int DEFAULT_SAMPLE_COUNT = 10;
  private static final int DEFAULT_OPS_OBJECT_SIZE = 4 * 1024;
  private static final int DEFAULT_TP_OBJECT_SIZE = 128 * 1024 * 1024;
  private static final int DEFAULT_MULTI_OPS_CLIENTS = 10;
  private static final int DEFAULT_MULTI_OPS_THREADS = 10;
  private static final int DEFAULT_MULTI_TP_CLIENTS = 2;
  private static final int DEFAULT_MULTI_TP_THREADS = 5;
  private static final int DEFAULT_MULTI_STREAM_RUNTIME_S = 60;

  private static final boolean DEFAULT_BENCHMARK_UNSIGNED = true;
  private static final boolean DEFAULT_BENCHMARK_PRESIGNED = true;
  private static final boolean DEFAULT_BENCHMARK_CHUNKED = true;
  private static final boolean DEFAULT_BENCHMARK_TP = true;
  private static final boolean DEFAULT_BENCHMARK_OPS = true;
  private static final boolean DEFAULT_BENCHMARK_GET = true;
  private static final boolean DEFAULT_BENCHMARK_SINGLE_STREAM = true;
  private static final boolean DEFAULT_BENCHMARK_MULTI_STREAM = true;
  private static final boolean DEFAULT_PATH_STYLE = true;
  private static final boolean DEFAULT_USE_SSL = false;

  private static final boolean DEFAULT_CREATE_BUCKET = true;
  private static final boolean DEFAULT_CLEAN_AFTER_RUN = false;

  private final Properties properties;

  Configuration(String path) {
    properties = new Properties();
    if (path != null && !path.isEmpty()) {
      try (FileInputStream fis = new FileInputStream(new File(path))) {
        properties.load(fis);
      } catch (IOException e) {
        System.err.println("Could not load properties from " + path + " because " + e);
      }
    } else {
      try (InputStream fis = getClass().getResourceAsStream(DEFAULT_URI)) {
        properties.load(fis);
      } catch (IOException e) {
        System.err.println("Could not load default properties from " + DEFAULT_URI + " because "
            + e);
      }
    }
  }

  boolean isClientMd5VerificationEnabled() {
    return getBooleanProperty("client_md5_verification", DEFAULT_CLIENT_MD5_VERIFICATION);
  }

  int getSampleCount() {
    return getIntProperty("sample_count", DEFAULT_SAMPLE_COUNT);
  }

  boolean benchmarkUnsigned() {
    return getBooleanProperty(Mode.UNSIGNED.propertiesKey, DEFAULT_BENCHMARK_UNSIGNED);
  }

  boolean benchmarkPresigned() {
    return getBooleanProperty(Mode.PRESIGNED.propertiesKey, DEFAULT_BENCHMARK_PRESIGNED);
  }

  boolean benchmarkChunked() {
    return getBooleanProperty(Mode.CHUNKED.propertiesKey, DEFAULT_BENCHMARK_CHUNKED);
  }

  boolean benchmarkTp() {
    return getBooleanProperty("benchmark_tp", DEFAULT_BENCHMARK_TP);
  }

  boolean benchmarkOps() {
    return getBooleanProperty("benchmark_ops", DEFAULT_BENCHMARK_OPS);
  }

  boolean benchmarkSingleStream() {
    return getBooleanProperty("benchmark_single_stream", DEFAULT_BENCHMARK_SINGLE_STREAM);
  }

  boolean benchmarkMultiStream() {
    return getBooleanProperty("benchmark_multi_stream", DEFAULT_BENCHMARK_MULTI_STREAM);
  }

  boolean benchmarkGet() {
    return getBooleanProperty("benchmark_get", DEFAULT_BENCHMARK_GET);
  }

  boolean pathStyle() {
    return getBooleanProperty("path_style", DEFAULT_PATH_STYLE);
  }

  boolean useSsl() {
    return getBooleanProperty("use_ssl", DEFAULT_USE_SSL);
  }

  boolean createBucket() {
    return getBooleanProperty("create_bucket", DEFAULT_CREATE_BUCKET);
  }


  boolean cleanAfterRun() {
    return getBooleanProperty("clean_after_run", DEFAULT_CLEAN_AFTER_RUN);
  }

  int getOpsObjectSize() {
    return getIntProperty("ops_object_size", DEFAULT_OPS_OBJECT_SIZE);
  }

  int getTpObjectSize() {
    return getIntProperty("tp_object_size", DEFAULT_TP_OBJECT_SIZE);
  }

  int getMultiOpsClients() {
    return getIntProperty("multi_stream_ops_clients", DEFAULT_MULTI_OPS_CLIENTS);
  }

  int getMultiOpsThreads() {
    return getIntProperty("multi_stream_ops_threads", DEFAULT_MULTI_OPS_THREADS);
  }

  int getMultiTpClients() {
    return getIntProperty("multi_stream_tp_clients", DEFAULT_MULTI_TP_CLIENTS);
  }

  int getMultiTpThreads() {
    return getIntProperty("multi_stream_tp_threads", DEFAULT_MULTI_TP_THREADS);
  }

  int getMultiStreamRuntimeS() {
    return getIntProperty("multi_stream_runtime_s", DEFAULT_MULTI_STREAM_RUNTIME_S);
  }

  private boolean getBooleanProperty(String key, boolean defaultValue) {
    try {
      return Boolean.valueOf((String) properties.get(key));
    } catch (Throwable t) {
      return defaultValue;
    }
  }

  private int getIntProperty(String key, int defaultValue) {
    try {
      return Integer.valueOf((String) properties.get(key));
    } catch (Throwable t) {
      return defaultValue;
    }
  }
}
