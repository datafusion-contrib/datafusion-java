package org.apache.arrow.datafusion;

import java.util.Optional;

/** Configures options specific to reading Parquet data */
@SuppressWarnings("UnusedReturnValue")
public class ParquetOptions {
  private final SessionConfig config;

  ParquetOptions(SessionConfig config) {
    this.config = config;
  }

  /**
   * Get whether parquet data page level metadata (Page Index) statistics are used
   *
   * @return whether using the page index is enabled
   */
  public boolean enablePageIndex() {
    return SessionConfig.getParquetOptionsEnablePageIndex(config.getPointer());
  }

  /**
   * Set whether to use parquet data page level metadata (Page Index) statistics to reduce the
   * number of rows decoded.
   *
   * @param enabled whether using the page index is enabled
   * @return the modified {@link ParquetOptions} instance
   */
  public ParquetOptions withEnablePageIndex(boolean enabled) {
    SessionConfig.setParquetOptionsEnablePageIndex(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get whether pruning is enabled, meaning reading row groups will be skipped based on metadata
   *
   * @return whether pruning is enabled
   */
  public boolean pruning() {
    return SessionConfig.getParquetOptionsPruning(config.getPointer());
  }

  /**
   * Set whether pruning is enabled, meaning reading row groups will be skipped based on metadata
   *
   * @param enabled whether to enable pruning
   * @return the modified {@link ParquetOptions} instance
   */
  public ParquetOptions withPruning(boolean enabled) {
    SessionConfig.setParquetOptionsPruning(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get whether file metadata is skipped, to avoid schema conflicts
   *
   * @return whether metadata is skipped
   */
  public boolean skipMetadata() {
    return SessionConfig.getParquetOptionsSkipMetadata(config.getPointer());
  }

  /**
   * Set whether file metadata is skipped, to avoid schema conflicts
   *
   * @param enabled whether to skip metadata
   * @return the modified {@link ParquetOptions} instance
   */
  public ParquetOptions withSkipMetadata(boolean enabled) {
    SessionConfig.setParquetOptionsSkipMetadata(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get the metadata size hint
   *
   * @return metadata size hint value
   */
  public Optional<Long> metadataSizeHint() {
    long sizeHint = SessionConfig.getParquetOptionsMetadataSizeHint(config.getPointer());
    return sizeHint < 0 ? Optional.empty() : Optional.of(sizeHint);
  }

  /**
   * Set the metadata size hint, which is used to attempt to read the full metadata at once rather
   * than needing one read to get the metadata size and then a second read for the metadata itself.
   *
   * @param metadataSizeHint the metadata size hint
   * @return the modified {@link ParquetOptions} instance
   */
  public ParquetOptions withMetadataSizeHint(Optional<Long> metadataSizeHint) {
    long value = -1L;
    if (metadataSizeHint.isPresent()) {
      value = metadataSizeHint.get();
      if (value < 0) {
        throw new RuntimeException("metadataSizeHint cannot be negative");
      }
    }
    SessionConfig.setParquetOptionsMetadataSizeHint(config.getPointer(), value);
    return this;
  }

  /**
   * Get whether filter pushdown is enabled, so filters are applied during parquet decoding
   *
   * @return whether filter pushdown is enabled
   */
  public boolean pushdownFilters() {
    return SessionConfig.getParquetOptionsPushdownFilters(config.getPointer());
  }

  /**
   * Set whether filter pushdown is enabled, so filters are applied during parquet decoding
   *
   * @param enabled whether to pushdown filters
   * @return the modified {@link ParquetOptions} instance
   */
  public ParquetOptions withPushdownFilters(boolean enabled) {
    SessionConfig.setParquetOptionsPushdownFilters(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get whether filter reordering is enabled to minimize evaluation cost
   *
   * @return whether filter reordering is enabled
   */
  public boolean reorderFilters() {
    return SessionConfig.getParquetOptionsReorderFilters(config.getPointer());
  }

  /**
   * Set whether filter reordering is enabled to minimize evaluation cost
   *
   * @param enabled whether to reorder filters
   * @return the modified {@link ParquetOptions} instance
   */
  public ParquetOptions withReorderFilters(boolean enabled) {
    SessionConfig.setParquetOptionsReorderFilters(config.getPointer(), enabled);
    return this;
  }
}
