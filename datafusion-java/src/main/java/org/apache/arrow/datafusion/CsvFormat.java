package org.apache.arrow.datafusion;

/** The CSV file format configuration */
public class CsvFormat extends AbstractProxy implements FileFormat {
  /** Create new CSV format with default options */
  public CsvFormat() {
    super(FileFormats.createCsv());
  }

  @Override
  void doClose(long pointer) {
    FileFormats.destroyFileFormat(pointer);
  }
}
