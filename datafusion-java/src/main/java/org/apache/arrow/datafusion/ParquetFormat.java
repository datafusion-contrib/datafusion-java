package org.apache.arrow.datafusion;

/** The Apache Parquet file format configuration */
public class ParquetFormat extends AbstractProxy implements FileFormat {
  /** Create new ParquetFormat with default options */
  public ParquetFormat() {
    super(FileFormats.createParquet());
  }

  @Override
  void doClose(long pointer) {
    FileFormats.destroyFileFormat(pointer);
  }
}
