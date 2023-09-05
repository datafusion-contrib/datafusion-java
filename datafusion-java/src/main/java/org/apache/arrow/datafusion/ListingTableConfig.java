package org.apache.arrow.datafusion;

import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/** Configuration for creating a {@link ListingTable} */
public class ListingTableConfig extends AbstractProxy implements AutoCloseable {
  /** A Builder for {@link ListingTableConfig} instances */
  public static class Builder {
    private final String[] tablePaths;
    private ListingOptions options = null;

    /**
     * Create a new {@link Builder}
     *
     * @param tablePath The path where data files are stored. This may be a file system path or a
     *     URL with a scheme. When no scheme is provided, glob expressions may be used to filter
     *     files.
     */
    public Builder(String tablePath) {
      this(new String[] {tablePath});
    }

    /**
     * Create a new {@link Builder}
     *
     * @param tablePaths The paths where data files are stored. This may be an array of file system
     *     paths or an array of URLs with a scheme. When no scheme is provided, glob expressions may
     *     be used to filter files.
     */
    public Builder(String[] tablePaths) {
      this.tablePaths = tablePaths;
    }

    /**
     * Specify the {@link ListingOptions} to use
     *
     * @param options The {@link ListingOptions} to use
     * @return this Builder instance
     */
    public Builder withListingOptions(ListingOptions options) {
      this.options = options;
      return this;
    }

    /**
     * Create the listing table config. This is async as the schema may need to be inferred
     *
     * @param context The {@link SessionContext} to use when inferring the schema
     * @return Future that will complete with the table config
     */
    public CompletableFuture<ListingTableConfig> build(SessionContext context) {
      return createListingTableConfig(this, context).thenApply(ListingTableConfig::new);
    }
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig}
   *
   * @param tablePath The path where data files are stored. This may be a file system path or a URL
   *     with a scheme. When no scheme is specified, glob expressions may be used to filter files.
   * @return A new {@link Builder} instance
   */
  public static Builder builder(String tablePath) {
    return new Builder(tablePath);
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig} from a file path
   *
   * @param tablePath The path where data files are stored
   * @return A new {@link Builder} instance
   */
  public static Builder builder(Path tablePath) {
    return new Builder(tablePath.toString());
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig} from an array of paths
   *
   * @param tablePaths The path array where data files are stored
   * @return A new {@link Builder} instance
   */
  public static Builder builder(Path[] tablePaths) {
    String[] pathStrings =
        Arrays.stream(tablePaths)
            .map(path -> path.toString())
            .toArray(length -> new String[length]);
    return new Builder(pathStrings);
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig} from a URI
   *
   * @param tablePath The location where data files are stored
   * @return A new {@link Builder} instance
   */
  public static Builder builder(URI tablePath) {
    return new Builder(tablePath.toString());
  }

  private ListingTableConfig(long pointer) {
    super(pointer);
  }

  private static CompletableFuture<Long> createListingTableConfig(
      Builder builder, SessionContext context) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    Runtime runtime = context.getRuntime();
    create(
        runtime.getPointer(),
        context.getPointer(),
        builder.tablePaths,
        builder.options == null ? 0 : builder.options.getPointer(),
        (errMessage, configId) -> {
          if (ErrorUtil.containsError(errMessage)) {
            future.completeExceptionally(new RuntimeException(errMessage));
          } else {
            future.complete(configId);
          }
        });
    return future;
  }

  @Override
  void doClose(long pointer) {
    destroy(pointer);
  }

  private static native void create(
      long runtime, long context, String[] tablePaths, long options, ObjectResultCallback callback);

  private static native void destroy(long pointer);
}
