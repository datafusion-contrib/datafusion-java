package org.apache.arrow.datafusion;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * A dataframe is a rectangle shaped data that holds columns and rows, and can be {@link
 * #collect(BufferAllocator) collected} into {@link
 * org.apache.arrow.vector.ipc.message.ArrowRecordBatch batches} and read via {@link ArrowReader
 * reader}.
 */
public interface DataFrame extends NativeProxy {
  /**
   * Collect dataframe into a list of record batches
   *
   * @param allocator {@link BufferAllocator buffer allocator} to allocate vectors within Reader
   * @return {@link ArrowReader reader} instance to extract the data, you are expected to {@link
   *     ArrowReader#close()} it after usage to release memory
   */
  CompletableFuture<ArrowReader> collect(BufferAllocator allocator);

  /**
   * Print results.
   *
   * @return null
   */
  CompletableFuture<Void> show();

  /**
   * Write results to a parquet file.
   *
   * @param path path to write parquet file to
   * @return null
   */
  CompletableFuture<Void> writeParquet(Path path);

  /**
   * Write results to a csv file.
   *
   * @param path path to write csv file to
   * @return null
   */
  CompletableFuture<Void> writeCsv(Path path);

  /**
   * Converts this DataFrame into a TableProvider that can be registered as a table view using
   * {@link SessionContext#registerParquet(String, Path)}
   *
   * @return the table provider ready to be e.g. {@link SessionContext#registerTable(String,
   *     TableProvider) registered}.
   */
  TableProvider intoView();
}
