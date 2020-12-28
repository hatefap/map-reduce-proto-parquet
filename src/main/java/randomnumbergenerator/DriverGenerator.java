package randomnumbergenerator;

import generatedproto.SahabRecords;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;

public class DriverGenerator {

    public static void main(String[] args) {
        int pageSize = 4 * 1024 * 1024;
        LongGenerator longGenerator = new LongGenerator(500_000_000L);
        Path filePath = new Path("minput/numbers.parquet");

        try (ParquetWriter<SahabRecords.Record> writer = new ProtoParquetWriter<>(filePath, SahabRecords.Record.class, CompressionCodecName.SNAPPY, 32*pageSize, pageSize)) {
            SahabRecords.Record.Builder recordBuilder = SahabRecords.Record.newBuilder();
            for (Long i : longGenerator) {
                recordBuilder.setNumber(i);
                writer.write(recordBuilder.build());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
