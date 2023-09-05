package org.apache.arrow.datafusion;

import java.util.concurrent.CompletableFuture;

/** A data source composed of multiple files that share a schema */
public class ListingTable extends AbstractProxy implements TableProvider {
  /**
   * Create a new listing table
   *
   * @param config The listing table configuration
   */
  public ListingTable(ListingTableConfig config) {
    super(createListingTable(config));
  }

  private static long createListingTable(ListingTableConfig config) {
    CompletableFuture<Long> result = new CompletableFuture<>();
    create(
        config.getPointer(),
        (errString, tableId) -> {
          if (ErrorUtil.containsError(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else {
            result.complete(tableId);
          }
        });
    return result.join();
  }

  @Override
  void doClose(long pointer) {
    TableProviders.destroyTableProvider(pointer);
  }

  private static native void create(long config, ObjectResultCallback result);
}
