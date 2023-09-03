package org.apache.arrow.datafusion;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultSessionContext extends AbstractProxy implements SessionContext {

  private static final Logger logger = LoggerFactory.getLogger(DefaultSessionContext.class);

  static native void querySql(
      long runtime, long context, String sql, ObjectResultCallback callback);

  static native void registerCsv(
      long runtime, long context, String name, String path, Consumer<String> callback);

  static native void registerParquet(
      long runtime, long context, String name, String path, Consumer<String> callback);

  static native long registerTable(long context, String name, long tableProvider) throws Exception;

  @Override
  public CompletableFuture<DataFrame> sql(String sql) {
    long runtime = getRuntime().getPointer();
    CompletableFuture<DataFrame> future = new CompletableFuture<>();
    querySql(
        runtime,
        getPointer(),
        sql,
        (errMessage, dataframeId) -> {
          if (null != errMessage && !errMessage.isEmpty()) {
            future.completeExceptionally(new RuntimeException(errMessage));
          } else {
            DefaultDataFrame frame = new DefaultDataFrame(DefaultSessionContext.this, dataframeId);
            future.complete(frame);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> registerCsv(String name, Path path) {
    long runtime = getRuntime().getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    registerCsv(
        runtime,
        getPointer(),
        name,
        path.toAbsolutePath().toString(),
        (errMessage) -> voidCallback(future, errMessage));
    return future;
  }

  @Override
  public CompletableFuture<Void> registerParquet(String name, Path path) {
    long runtime = getRuntime().getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    registerParquet(
        runtime,
        getPointer(),
        name,
        path.toAbsolutePath().toString(),
        (errMessage) -> voidCallback(future, errMessage));
    return future;
  }

  @Override
  public Optional<TableProvider> registerTable(String name, TableProvider tableProvider)
      throws Exception {
    long previouslyRegistered = registerTable(getPointer(), name, tableProvider.getPointer());
    if (previouslyRegistered == 0) {
      return Optional.empty();
    }
    return Optional.of(new DefaultTableProvider(previouslyRegistered));
  }

  private void voidCallback(CompletableFuture<Void> future, String errMessage) {
    if (null != errMessage && !errMessage.isEmpty()) {
      future.completeExceptionally(new RuntimeException(errMessage));
    } else {
      future.complete(null);
    }
  }

  @Override
  public Runtime getRuntime() {
    return runtime;
  }

  private final TokioRuntime runtime;

  DefaultSessionContext(long pointer) {
    super(pointer);
    this.runtime = TokioRuntime.create();
    registerChild(runtime);
  }

  @Override
  void doClose(long pointer) throws Exception {
    SessionContexts.destroySessionContext(pointer);
  }
}
