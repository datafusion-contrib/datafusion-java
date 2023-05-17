package org.apache.arrow.datafusion;

import java.util.concurrent.CompletableFuture;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

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
        (errString, arrowArrayAddress) -> {
          if (containsError(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else if (arrowArrayAddress == 0) {
            // Reached end of stream
            result.complete(false);
          } else {
            try {
              ArrowArray arrowArray = ArrowArray.wrap(arrowArrayAddress);
              Data.importIntoVectorSchemaRoot(allocator, arrowArray, vectorSchemaRoot, null);
              result.complete(true);
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
        (errString, arrowSchemaAddress) -> {
          if (containsError(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else {
            try {
              ArrowSchema arrowSchema = ArrowSchema.wrap(arrowSchemaAddress);
              Schema schema = Data.importSchema(allocator, arrowSchema, null);
              result.complete(schema);
              // The FFI schema will be released from rust when it is dropped
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

  private static native void getSchema(long pointer, ObjectResultCallback callback);

  private static native void next(long runtime, long pointer, ObjectResultCallback callback);

  private static native void destroy(long pointer);
}
