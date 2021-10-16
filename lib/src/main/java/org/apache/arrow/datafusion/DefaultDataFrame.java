package org.apache.arrow.datafusion;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
            try (RootAllocator allocator = new RootAllocator();
                ByteArrayReadableSeekableByteChannel byteChannel =
                    new ByteArrayReadableSeekableByteChannel(arr);
                ArrowFileReader reader = new ArrowFileReader(byteChannel, allocator)) {
              logger.info("before reader loading {}", reader);
              try {
                //                boolean loaded = reader.loadNextBatch();
                //                logger.info("reader {} loaded {}", reader, loaded);
              } catch (Throwable e) {
                //                System.out.printf("%s", e.getStackTrace());
                throw e;
              }
              VectorSchemaRoot root = reader.getVectorSchemaRoot();
              Schema schema = root.getSchema();
              logger.info("schema {}", schema);
              Float8Vector vector = (Float8Vector) root.getVector(0);
              logger.info(
                  "vector size {}, row count {}", vector.getValueCount(), root.getRowCount());
              for (int i = 0; i < root.getRowCount(); i += 1) {
                logger.info("value {}={}", i, vector.getValueAsDouble(i));
              }
            } catch (Exception e) {
              logger.warn("failed to read", e);
            }
            result.complete(null);
          }
          logger.info("returning null");
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
