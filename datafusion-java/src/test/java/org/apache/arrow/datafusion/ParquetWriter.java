package org.apache.arrow.datafusion;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

/** Helper class for writing test files in Parquet format using Avro records */
public class ParquetWriter {
  public static void writeParquet(
      Path path, String schema, int rowCount, BiConsumer<Integer, GenericData.Record> setRecord)
      throws IOException {
    Configuration config = new Configuration();
    org.apache.hadoop.fs.Path hadoopFilePath = new org.apache.hadoop.fs.Path(path.toString());
    OutputFile outputFile = HadoopOutputFile.fromPath(hadoopFilePath, config);

    Schema.Parser parser = new Schema.Parser().setValidate(true);
    Schema avroSchema = parser.parse(schema);

    try (org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer =
        AvroParquetWriter.<GenericData.Record>builder(outputFile)
            .withSchema(avroSchema)
            .withConf(config)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
      for (int i = 0; i < rowCount; ++i) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        setRecord.accept(i, record);
        writer.write(record);
      }
    }
  }
}
