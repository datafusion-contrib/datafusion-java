package org.apache.arrow.datafusion;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/** A session context holds resources and is the entrance for obtaining {@link DataFrame} */
public interface SessionContext extends AutoCloseable, NativeProxy {

  /**
   * Obtain the {@link DataFrame} by running the {@code sql} against the datafusion library
   *
   * @param sql The query to execute
   * @return DataFrame representing the query result
   */
  CompletableFuture<DataFrame> sql(String sql);

  /**
   * Registering a csv file with the context
   *
   * @param name The table name to use to refer to the data
   * @param path Path to the CSV file
   * @return Future that is completed when the CSV is registered
   */
  CompletableFuture<Void> registerCsv(String name, Path path);

  /**
   * Registering a parquet file with the context
   *
   * @param name The table name to use to refer to the data
   * @param path Path to the Parquet file
   * @return Future that is completed when the Parquet file is registered
   */
  CompletableFuture<Void> registerParquet(String name, Path path);

  /**
   * Get the runtime associated with this context
   *
   * @return The context's runtime
   */
  Runtime getRuntime();
}
