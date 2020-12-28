package sortparquet;

import com.google.protobuf.Message;
import generatedproto.SahabRecords;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetOutputFormat;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigInteger;

public class SortParquet extends Configured implements Tool {

    public static final int NUMBER_OF_REDUCERS = 30;
    public static final int PAGE_SIZE = 32 * 1024 * 1024;
    public static final int BLOCK_SIZE = 5 * PAGE_SIZE;

    public static class ParquetMap extends Mapper<Void, Group, LongDescendComparable, NullWritable>{
        public void map(Void key, Group value, Context context) throws IOException, InterruptedException {
            Type type = value.getType().getFields().get(0);
            // what is index?
            long val = value.getLong(type.getName(), 0);
            context.write(new LongDescendComparable(val), NullWritable.get());
        }

    }

    public static class ParquetReducer extends Reducer<LongDescendComparable, NullWritable, Void, Message>{
        private final SahabRecords.Record.Builder recordBuilder = SahabRecords.Record.newBuilder();
        @Override
        public void reduce(LongDescendComparable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                context.write(null, recordBuilder.setNumber(key.get()).build());

        }
    }

    public static class ParquetPartitioner extends Partitioner <LongDescendComparable, NullWritable> {
        private final BigInteger M = new BigInteger(String.valueOf(Long.MAX_VALUE));
        private final BigInteger m = new BigInteger(String.valueOf(Long.MIN_VALUE));
        private final BigInteger diff = M.subtract(m);
        @Override
        public int getPartition(LongDescendComparable longWritable, NullWritable nullWritable, int numReduceTasks) {
            if(numReduceTasks < 2){
                return 0;
            }
            else {
                BigInteger bucketLength = diff.divide(BigInteger.valueOf(numReduceTasks));
                BigInteger t = new BigInteger(String.valueOf(longWritable.get()));
                int bucket = (int) Math.floor(t.subtract(m).divide(bucketLength).longValue());

                return (numReduceTasks - 1) - bucket;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        int statusCode = ToolRunner.run(new SortParquet(), args);
        System.exit(statusCode);

    }

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2){
            System.err.println("Invalid Command");
            System.exit(-1);
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("sort parquet file");
        job.setJarByClass(SortParquet.class);

        job.setMapOutputKeyClass(LongDescendComparable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(SpecificRecord.class);

        job.setNumReduceTasks(NUMBER_OF_REDUCERS);

        job.setMapperClass(ParquetMap.class);
        job.setPartitionerClass(ParquetPartitioner.class);
        job.setReducerClass(ParquetReducer.class);

        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(ProtoParquetOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        ProtoParquetOutputFormat.setOutputPath(job, new Path(args[1]));

        ProtoParquetOutputFormat.setProtobufClass(job, SahabRecords.Record.class);
        ProtoParquetOutputFormat.setBlockSize(job, BLOCK_SIZE);
        ProtoParquetOutputFormat.setPageSize(job, PAGE_SIZE);
        ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);


        return job.waitForCompletion(true) ? 0 : 1;
    }


}
