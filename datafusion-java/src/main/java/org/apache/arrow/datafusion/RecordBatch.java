package org.apache.arrow.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.vector.FieldVector;

public interface RecordBatch {

    int getNumColumns();

    FieldVector getColumn(int index);
}
