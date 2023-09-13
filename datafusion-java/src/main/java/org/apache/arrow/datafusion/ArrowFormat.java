package org.apache.arrow.datafusion;

/** The Apache Arrow IPC file format configuration. This format is also known as Feather V2 */
public class ArrowFormat extends AbstractProxy implements FileFormat {
  /** Create a new ArrowFormat with default options */
  public ArrowFormat() {
    super(FileFormats.createArrow());
  }

  @Override
  void doClose(long pointer) {
    FileFormats.destroyFileFormat(pointer);
  }
}
