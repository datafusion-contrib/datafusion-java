package org.apache.arrow.datafusion;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultDataFrame extends AbstractProxy implements DataFrame {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDataFrame.class);
    private final DefaultSessionContext context;

    DefaultDataFrame(DefaultSessionContext context, long pointer) {
        super(pointer);
        this.context = context;
        registerChild(context);
    }

    @Override
    public CompletableFuture<List<RecordBatch>> collect() {
        CompletableFuture<List<RecordBatch>> result = new CompletableFuture<>();
        Runtime runtime = context.getRuntime();
        long runtimePointer = runtime.getPointer();
        long dataframe = getPointer();
        DataFrames.collectDataframe(
            runtimePointer,
            dataframe,
            (String errString, long[] arr) -> {
                if (containsError(errString)) {
                    result.completeExceptionally(new RuntimeException(errString));
                } else {
                    logger.info("successfully completed with arr length={}", arr.length);
                    List<RecordBatch> recordBatches = Arrays.stream(arr).mapToObj(pointer -> new DefaultRecordBatch(pointer, DefaultDataFrame.this)).collect(Collectors.toList());
                    result.complete(recordBatches);
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
    void doClose(long pointer) {
        DataFrames.destroyDataFrame(pointer);
    }
}
