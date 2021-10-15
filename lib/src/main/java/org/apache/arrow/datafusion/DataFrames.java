package org.apache.arrow.datafusion;

import java.util.function.Function;

final class DataFrames {

  private DataFrames() {}

  static native void destroyDataFrame(long pointer);

  static native void showDataframe(long runtime, long dataframe, Function<String, Void> callback);
}
