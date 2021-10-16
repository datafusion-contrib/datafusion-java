package org.apache.arrow.datafusion;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
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
        (String errString, byte[] arr) -> {
          if (errString != null && !"".equals(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else {
            logger.info("successfully completed with arr length={}", arr.length);
            logger.info("begin to decode");
            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ByteArrayReadableSeekableByteChannel byteChannel =
                    new ByteArrayReadableSeekableByteChannel(arr);
                ArrowFileReader reader = new ArrowFileReader(byteChannel, allocator)) {
              logger.info("reader {}", reader);
              Schema schema = reader.getVectorSchemaRoot().getSchema();
              logger.info("schema {}", schema);
            } catch (IOException e) {
              logger.warn("failed to read", e);
            } catch (Exception e) {
              System.out.println(e);
              logger.warn("unexpected exception", e);
            }
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
