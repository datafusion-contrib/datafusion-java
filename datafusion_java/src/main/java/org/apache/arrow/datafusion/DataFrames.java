package org.apache.arrow.datafusion;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class DataFrames {

  private DataFrames() {}

  static native void destroyDataFrame(long pointer);

  static native void showDataframe(long runtime, long dataframe, Consumer<String> callback);

  static native void collectDataframe(
      long runtime, long dataframe, BiConsumer<String, byte[]> callback);
}
