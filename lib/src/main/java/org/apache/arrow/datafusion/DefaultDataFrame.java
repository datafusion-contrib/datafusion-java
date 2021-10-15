package org.apache.arrow.datafusion;

import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.ipc.ArrowReader;

class DefaultDataFrame extends AbstractProxy implements DataFrame {

  private final ExecutionContext context;

  DefaultDataFrame(ExecutionContext context, long pointer) {
    super(pointer);
    this.context = context;
  }

  @Override
  public ArrowReader getReader() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public CompletableFuture<Void> show() {
    Runtime runtime = context.getRuntime();
    long runtimePointer = runtime.getPointer();
    long dataframe = getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    DataFrames.showDataframe(
        runtimePointer,
        dataframe,
        (String errString) -> {
          if (errString != null && !"".equals(errString)) {
            future.completeExceptionally(new RuntimeException(errString));
          } else {
            future.complete(null);
          }
          return /*void*/ null;
        });
    return future;
  }

  @Override
  void doClose(long pointer) {
    DataFrames.destroyDataFrame(pointer);
  }
}
