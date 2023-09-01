package org.apache.arrow.datafusion;

class TableProviders {

  private TableProviders() {}

  static native void destroyTableProvider(long pointer);
}
