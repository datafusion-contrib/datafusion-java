package org.apache.arrow.datafusion;

public class DefaultRecordBatch extends AbstractProxy implements RecordBatch {

    private final DefaultDataFrame dataFrame;

    DefaultRecordBatch(long pointer, DefaultDataFrame dataFrame) {
        super(pointer);
        this.dataFrame = dataFrame;
        registerChild(dataFrame);
    }

    @Override
    void doClose(long pointer) throws Exception {
        RecordBatches.destroyRecordBatch(pointer);
    }
}
