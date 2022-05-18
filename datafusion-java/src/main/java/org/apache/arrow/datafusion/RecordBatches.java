package org.apache.arrow.datafusion;

final class RecordBatches {

    private RecordBatches() {
    }

    static native void destroyRecordBatch(long pointer);
}
