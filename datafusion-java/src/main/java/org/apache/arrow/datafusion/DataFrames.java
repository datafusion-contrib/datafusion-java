package org.apache.arrow.datafusion;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** helper class that calls into native stack for {@link DataFrame} */
final class DataFrames {

  private DataFrames() {}

  static native void destroyDataFrame(long pointer);

  static native void showDataframe(long runtime, long dataframe, Consumer<String> callback);

  static native void collectDataframe(
      long runtime, long dataframe, BiConsumer<String, long[]> callback);
}
