package org.apache.arrow.datafusion;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultDataFrame extends AbstractProxy implements DataFrame {

  private static final Logger logger = LoggerFactory.getLogger(DefaultDataFrame.class);
  private final SessionContext context;

  DefaultDataFrame(SessionContext context, long pointer) {
    super(pointer);
    this.context = context;
  }

  @Override
  public CompletableFuture<ArrowReader> collect(BufferAllocator allocator) {
    CompletableFuture<ArrowReader> result = new CompletableFuture<>();
    Runtime runtime = context.getRuntime();
    long runtimePointer = runtime.getPointer();
    long dataframe = getPointer();
    DataFrames.collectDataframe(
        runtimePointer,
        dataframe,
        (String errString, byte[] arr) -> {
          if (containsError(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else {
            logger.info("successfully completed with arr length={}", arr.length);
            ByteArrayReadableSeekableByteChannel byteChannel =
                new ByteArrayReadableSeekableByteChannel(arr);
            result.complete(new ArrowFileReader(byteChannel, allocator));
          }
        });
    return result;
  }

  private boolean containsError(String errString) {
    return errString != null && !"".equals(errString);
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
          if (containsError(errString)) {
            future.completeExceptionally(new RuntimeException(errString));
          } else {
            future.complete(null);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> writeParquet(Path path) {
    Runtime runtime = context.getRuntime();
    long runtimePointer = runtime.getPointer();
    long dataframe = getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    DataFrames.writeParquet(
        runtimePointer,
        dataframe,
        path.toAbsolutePath().toString(),
        (String errString) -> {
          if (containsError(errString)) {
            future.completeExceptionally(new RuntimeException(errString));
          } else {
            future.complete(null);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> writeCsv(Path path) {
    Runtime runtime = context.getRuntime();
    long runtimePointer = runtime.getPointer();
    long dataframe = getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    DataFrames.writeCsv(
        runtimePointer,
        dataframe,
        path.toAbsolutePath().toString(),
        (String errString) -> {
          if (containsError(errString)) {
            future.completeExceptionally(new RuntimeException(errString));
          } else {
            future.complete(null);
          }
        });
    return future;
  }

  public CompletableFuture<Void> registerTable(SessionContext ctx, String name) {
    Runtime runtime = context.getRuntime();
    long runtimePointer = runtime.getPointer();
    long dataframe = getPointer();
    long contextPointer = ctx.getPointer();
    CompletableFuture<Void> future = new CompletableFuture<>();
    DataFrames.registerTable(
        runtimePointer,
        dataframe,
        contextPointer,
        name,
        (String errString) -> {
          if (containsError(errString)) {
            future.completeExceptionally(new RuntimeException(errString));
          } else {
            future.complete(null);
          }
        });
    return future;
  }

  @Override
  void doClose(long pointer) {
    DataFrames.destroyDataFrame(pointer);
  }
}
