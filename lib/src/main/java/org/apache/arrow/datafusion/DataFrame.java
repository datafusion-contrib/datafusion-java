package org.apache.arrow.datafusion;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public interface DataFrame extends NativeProxy {
  /** Collect dataframe into a list of record batches */
  CompletableFuture<List<ArrowRecordBatch>> collect();

  /** Print results. */
  CompletableFuture<Void> show();
}
