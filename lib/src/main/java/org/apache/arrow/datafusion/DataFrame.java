package org.apache.arrow.datafusion;

import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.ipc.ArrowReader;

public interface DataFrame extends NativeProxy {
  ArrowReader getReader();

  /** Print results. */
  CompletableFuture<Void> show();
}
