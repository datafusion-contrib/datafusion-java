package org.apache.arrow.datafusion;

class FileFormats {

  private FileFormats() {}

  static native long createCsv();

  static native long createParquet();

  static native void destroyFileFormat(long pointer);
}
