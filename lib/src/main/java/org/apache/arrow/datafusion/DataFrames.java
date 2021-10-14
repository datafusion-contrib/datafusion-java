package org.apache.arrow.datafusion;

final class DataFrames {

  private DataFrames() {}

  static native void destroyDataFrame(long pointer);
}
