package org.apache.arrow.datafusion;

import java.util.function.Consumer;

/** Configuration for creating a {@link SessionContext} using {@link SessionContexts#withConfig} */
public class SessionConfig extends AbstractProxy implements AutoCloseable {
  /** Create a new default {@link SessionConfig} */
  public SessionConfig() {
    super(create());
  }

  /**
   * Get options related to query execution
   *
   * @return {@link ExecutionOptions} instance for this config
   */
  public ExecutionOptions executionOptions() {
    return new ExecutionOptions(this);
  }

  /**
   * Get options specific to parsing SQL queries
   *
   * @return {@link SqlParserOptions} instance for this config
   */
  public SqlParserOptions sqlParserOptions() {
    return new SqlParserOptions(this);
  }

  /**
   * Modify this session configuration and then return it, to simplify use in a try-with-resources
   * statement
   *
   * @param configurationCallback Callback used to update the configuration
   * @return This {@link SessionConfig} instance after being updated
   */
  public SessionConfig withConfiguration(Consumer<SessionConfig> configurationCallback) {
    configurationCallback.accept(this);
    return this;
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native long create();

  private static native void destroy(long pointer);

  // ExecutionOptions native methods

  static native long getExecutionOptionsBatchSize(long pointer);

  static native void setExecutionOptionsBatchSize(long pointer, long batchSize);

  static native boolean getExecutionOptionsCoalesceBatches(long pointer);

  static native void setExecutionOptionsCoalesceBatches(long pointer, boolean enabled);

  static native boolean getExecutionOptionsCollectStatistics(long pointer);

  static native void setExecutionOptionsCollectStatistics(long pointer, boolean enabled);

  static native long getExecutionOptionsTargetPartitions(long pointer);

  static native void setExecutionOptionsTargetPartitions(long pointer, long batchSize);

  // ParquetOptions native methods

  static native boolean getParquetOptionsEnablePageIndex(long pointer);

  static native void setParquetOptionsEnablePageIndex(long pointer, boolean enabled);

  static native boolean getParquetOptionsPruning(long pointer);

  static native void setParquetOptionsPruning(long pointer, boolean enabled);

  static native boolean getParquetOptionsSkipMetadata(long pointer);

  static native void setParquetOptionsSkipMetadata(long pointer, boolean enabled);

  static native long getParquetOptionsMetadataSizeHint(long pointer);

  static native void setParquetOptionsMetadataSizeHint(long pointer, long value);

  static native boolean getParquetOptionsPushdownFilters(long pointer);

  static native void setParquetOptionsPushdownFilters(long pointer, boolean enabled);

  static native boolean getParquetOptionsReorderFilters(long pointer);

  static native void setParquetOptionsReorderFilters(long pointer, boolean enabled);

  // SqlParserOptions native methods

  static native boolean getSqlParserOptionsParseFloatAsDecimal(long pointer);

  static native void setSqlParserOptionsParseFloatAsDecimal(long pointer, boolean enabled);

  static native boolean getSqlParserOptionsEnableIdentNormalization(long pointer);

  static native void setSqlParserOptionsEnableIdentNormalization(long pointer, boolean enabled);

  static native String getSqlParserOptionsDialect(long pointer);

  static native void setSqlParserOptionsDialect(long pointer, String dialect);
}
