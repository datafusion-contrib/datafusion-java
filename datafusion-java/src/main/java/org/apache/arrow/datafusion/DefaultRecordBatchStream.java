package org.apache.arrow.datafusion;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.util.TransferPair;

class DefaultRecordBatchStream extends AbstractProxy implements RecordBatchStream {
  private final SessionContext context;
  private final BufferAllocator allocator;
  private VectorSchemaRoot vectorSchemaRoot = null;
  private boolean initialized = false;

  DefaultRecordBatchStream(SessionContext context, long pointer, BufferAllocator allocator) {
    super(pointer);
    this.context = context;
    this.allocator = allocator;
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
    if (initialized) {
      vectorSchemaRoot.close();
    }
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() {
    ensureInitialized();
    return vectorSchemaRoot;
  }

  @Override
  public CompletableFuture<Boolean> loadNextBatch() {
    ensureInitialized();
    Runtime runtime = context.getRuntime();
    long runtimePointer = runtime.getPointer();
    long recordBatchStream = getPointer();
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    next(
        runtimePointer,
        recordBatchStream,
        (String errString, byte[] arr) -> {
          if (containsError(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else if (arr.length == 0) {
            // Reached end of stream
            result.complete(false);
          } else {
            ByteArrayReadableSeekableByteChannel byteChannel =
                new ByteArrayReadableSeekableByteChannel(arr);
            try (ArrowFileReader reader = new ArrowFileReader(byteChannel, allocator)) {
              VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
              if (!reader.loadNextBatch()) {
                result.completeExceptionally(new RuntimeException("No record batch from reader"));
              } else {
                // Transfer data into our VectorSchemaRoot
                List<FieldVector> vectors = batchRoot.getFieldVectors();
                for (int i = 0; i < vectors.size(); ++i) {
                  TransferPair pair =
                      vectors.get(i).makeTransferPair(vectorSchemaRoot.getVector(i));
                  pair.transfer();
                }
                vectorSchemaRoot.setRowCount(batchRoot.getRowCount());
                result.complete(true);
              }
            } catch (Exception e) {
              result.completeExceptionally(e);
            }
          }
        });
    return result;
  }

  private void ensureInitialized() {
    if (!initialized) {
      Schema schema = getSchema();
      this.vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
    }
    initialized = true;
  }

  private Schema getSchema() {
    long recordBatchStream = getPointer();
    // Native method is not async, but use a future to store the result for convenience
    CompletableFuture<Schema> result = new CompletableFuture<>();
    getSchema(
        recordBatchStream,
        (String errString, byte[] arr) -> {
          if (containsError(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else {
            ByteArrayReadableSeekableByteChannel byteChannel =
                new ByteArrayReadableSeekableByteChannel(arr);
            try (ArrowFileReader reader = new ArrowFileReader(byteChannel, allocator)) {
              result.complete(reader.getVectorSchemaRoot().getSchema());
            } catch (Exception e) {
              result.completeExceptionally(e);
            }
          }
        });
    return result.join();
  }

  private static boolean containsError(String errString) {
    return errString != null && !"".equals(errString);
  }

  private static native void getSchema(long pointer, BiConsumer<String, byte[]> callback);

  private static native void next(long runtime, long pointer, BiConsumer<String, byte[]> callback);

  private static native void destroy(long pointer);
}
