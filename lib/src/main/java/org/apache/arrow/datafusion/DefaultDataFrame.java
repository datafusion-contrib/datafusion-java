package org.apache.arrow.datafusion;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class DefaultDataFrame extends AbstractProxy implements DataFrame {

  private static final Logger logger = LogManager.getLogger(DefaultDataFrame.class);
  private final ExecutionContext context;

  DefaultDataFrame(ExecutionContext context, long pointer) {
    super(pointer);
    this.context = context;
  }

  @Override
  public CompletableFuture<List<ArrowRecordBatch>> collect() {
    CompletableFuture<List<ArrowRecordBatch>> result = new CompletableFuture<>();
    Runtime runtime = context.getRuntime();
    long runtimePointer = runtime.getPointer();
    long dataframe = getPointer();
    DataFrames.collectDataframe(
        runtimePointer,
        dataframe,
        (String errString, Object[] arr) -> {
          if (errString != null && !"".equals(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else {
            logger.info("successfully completed with {} arr", arr.length);
//            for (Object x : arr) {
//              byte[] bytes = (byte[]) x;
//              logger.info("array element {}", new String(bytes, StandardCharsets.UTF_8));
//            }
            result.complete(null);
          }
          return /*void*/ null;
        });
    return result;
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
