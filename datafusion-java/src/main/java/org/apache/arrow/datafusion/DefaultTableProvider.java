package org.apache.arrow.datafusion;

class DefaultTableProvider extends AbstractProxy implements TableProvider {
  DefaultTableProvider(long pointer) {
    super(pointer);
  }

  @Override
  void doClose(long pointer) throws Exception {
    TableProviders.destroyTableProvider(pointer);
  }
}
