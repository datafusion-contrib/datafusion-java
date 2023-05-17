package org.apache.arrow.datafusion;

import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A record batch stream is a stream of tabular Arrow data that can be iterated over asynchronously
 */
public interface RecordBatchStream extends AutoCloseable, NativeProxy {
  /**
   * Get the VectorSchemaRoot that will be populated with data as the stream is iterated over
   *
   * @return the stream's VectorSchemaRoot
   */
  VectorSchemaRoot getVectorSchemaRoot();

  /**
   * Load the next record batch in the stream into the VectorSchemaRoot
   *
   * @return Future that will complete with true if a batch was loaded or false if the end of the
   *     stream has been reached
   */
  CompletableFuture<Boolean> loadNextBatch();
}
