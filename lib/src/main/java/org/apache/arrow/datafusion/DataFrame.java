package org.apache.arrow.datafusion;

import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public interface DataFrame extends NativeProxy {
  /** Collect dataframe into a list of record batches */
  CompletableFuture<ArrowReader> collect(BufferAllocator allocator);

  /** Print results. */
  CompletableFuture<Void> show();
}
