package org.apache.arrow.datafusion;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/** An execution context holds resources and is the entrance for obtaining {@link DataFrame} */
public interface ExecutionContext extends AutoCloseable, NativeProxy {

  /** Obtain the {@link DataFrame} by running the {@code sql} against the datafusion library */
  CompletableFuture<DataFrame> sql(String sql);

  /** Registering a csv file with the context */
  CompletableFuture<Void> registerCsv(String name, Path path);

  /** Registering a parquet file with the context */
  CompletableFuture<Void> registerParquet(String name, Path path);

  /** Get the runtime associated with this context */
  Runtime getRuntime();
}
