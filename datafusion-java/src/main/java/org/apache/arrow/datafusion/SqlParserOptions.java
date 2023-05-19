package org.apache.arrow.datafusion;

/** Configures options specific to parsing SQL queries */
@SuppressWarnings("UnusedReturnValue")
public class SqlParserOptions {
  private final SessionConfig config;

  SqlParserOptions(SessionConfig config) {
    this.config = config;
  }

  /**
   * Get whether to parse floats as decimal type
   *
   * @return whether to parse floats as decimal
   */
  public boolean parseFloatAsDecimal() {
    return SessionConfig.getSqlParserOptionsParseFloatAsDecimal(config.getPointer());
  }

  /**
   * Set whether to parse floats as decimal type
   *
   * @param enabled whether to parse floats as decimal
   * @return the modified {@link SqlParserOptions} instance
   */
  public SqlParserOptions withParseFloatAsDecimal(boolean enabled) {
    SessionConfig.setSqlParserOptionsParseFloatAsDecimal(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get whether to convert identifiers to lowercase when not quoted
   *
   * @return whether ident normalization is enabled
   */
  public boolean enableIdentNormalization() {
    return SessionConfig.getSqlParserOptionsEnableIdentNormalization(config.getPointer());
  }

  /**
   * Set whether to convert identifiers to lowercase when not quoted
   *
   * @param enabled whether ident normalization is enabled
   * @return the modified {@link SqlParserOptions} instance
   */
  public SqlParserOptions withEnableIdentNormalization(boolean enabled) {
    SessionConfig.setSqlParserOptionsEnableIdentNormalization(config.getPointer(), enabled);
    return this;
  }

  /**
   * Get the SQL dialect used
   *
   * @return the SQL dialect used
   */
  public String dialect() {
    return SessionConfig.getSqlParserOptionsDialect(config.getPointer());
  }

  /**
   * Set the SQL dialect to use
   *
   * @param dialect the SQL dialect to use
   * @return the modified {@link SqlParserOptions} instance
   */
  public SqlParserOptions withDialect(String dialect) {
    SessionConfig.setSqlParserOptionsDialect(config.getPointer(), dialect);
    return this;
  }
}
