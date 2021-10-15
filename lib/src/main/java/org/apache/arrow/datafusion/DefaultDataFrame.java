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
    throw new UnsupportedOperationException("show not implemented");
  }

  @Override
  void doClose(long pointer) {
    DataFrames.destroyDataFrame(pointer);
  }
}
