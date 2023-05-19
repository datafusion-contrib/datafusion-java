package org.apache.arrow.datafusion;

/** Configures options related to query execution */
@SuppressWarnings("UnusedReturnValue")
public class ExecutionOptions {
  private final SessionConfig config;

  ExecutionOptions(SessionConfig config) {
    this.config = config;
  }

  /**
   * Get execution options related to reading Parquet data
   *
   * @return {@link ParquetOptions} instance for this config
   */
  public ParquetOptions parquet() {
    return new ParquetOptions(config);
  }

  /**
   * Get the batch size
   *
   * @return batch size
   */
  public long batchSize() {
    return SessionConfig.getExecutionOptionsBatchSize(config.getPointer());
  }

  /**
   * Set the size of batches to use when creating new data batches
   *
   * @param batchSize the batch size to set
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withBatchSize(long batchSize) {
    SessionConfig.setExecutionOptionsBatchSize(config.getPointer(), batchSize);
    return this;
  }

  /**
   * Get whether batch coalescing is enabled
   *
   * @return whether batch coalescing is enabled
   */
  public boolean coalesceBatches() {
    return SessionConfig.getExecutionOptionsCoalesceBatches(config.getPointer());
  }

  /**
   * Set whether to enable batch coalescing
   *
   * @param enabled whether to enable batch coalescing
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withCoalesceBatches(boolean enabled) {
    SessionConfig.setExecutionOptionsCoalesceBatches(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get whether statistics collection is enabled
   *
   * @return whether statistics collection is enabled
   */
  public boolean collectStatistics() {
    return SessionConfig.getExecutionOptionsCollectStatistics(config.getPointer());
  }

  /**
   * Set whether to enable statistics collection
   *
   * @param enabled whether to enable statistics collection
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withCollectStatistics(boolean enabled) {
    SessionConfig.setExecutionOptionsCollectStatistics(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get the target number of partitions
   *
   * @return number of partitions
   */
  public long targetPartitions() {
    return SessionConfig.getExecutionOptionsTargetPartitions(config.getPointer());
  }

  /**
   * Set the target number of partitions
   *
   * @param targetPartitions the number of partitions to set
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withTargetPartitions(long targetPartitions) {
    SessionConfig.setExecutionOptionsTargetPartitions(config.getPointer(), targetPartitions);
    return this;
  }
}
