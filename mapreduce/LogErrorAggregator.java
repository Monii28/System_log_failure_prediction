import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogErrorAggregator {

    public static class LogMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Skip CSV Header lines
            if (line.startsWith("LineId") || line.trim().isEmpty()) {
                return;
            }

            // Parse CSV lines keeping comma-separated components intact
            String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            // Identify source file based on path characteristics (HDFS vs BGL)
            String filePath = ((FileSplit) context.getInputSplit()).getPath().toString().toLowerCase();
            String source = "UNKNOWN";
            String dateHour = "UNKNOWN";
            String errorType = "UNKNOWN";

            try {
                  if (filePath.contains("hdfs_logs")) {
                       source = "HDFS";
                       // For HDFS_2k.log_structured.csv: Date/Time is at Index 1, Error Level is at Index 5
                       String rawDate = tokens[1].trim();
                       dateHour = formatHdfsDate(rawDate); 
                       errorType = tokens[5].trim();
                  } else if (filePath.contains("bgl_logs")) {
                       source = "BGL";
                       // For BGL_2k.log_structured.csv: Level is at Index 1, Timestamp is at Index 4
                       String rawTimestamp = tokens[4].trim();
                      if (rawTimestamp.length() >= 13) {
                           dateHour = rawTimestamp.substring(0, 10) + " " + rawTimestamp.substring(11, 13);
                      } else {
                           dateHour = rawTimestamp;
                      }
                      errorType = tokens[1].trim();
                }

                // Emit schema key: Date_Hour,Error_Type,Source
                outputKey.set(dateHour + "," + errorType + "," + source);
                context.write(outputKey, one);

            } catch (Exception e) {
                // Ignore improperly structured logs rows to guarantee framework resilience
            }
        }

        private String formatHdfsDate(String rawDate) {
            // Converts '081109 203518' split variants safely or returns raw format
            if (rawDate.length() >= 6) {
                // Convert YYMMDD sequence to ISO standard layout
                return "20" + rawDate.substring(0,2) + "-" + rawDate.substring(2,4) + "-" + rawDate.substring(4,6);
            }
            return rawDate;
        }
    }

    public static class LogReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // Calculate total occurrences [cite: 91]
            }
            outputValue.set(String.valueOf(sum));
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LogErrorAggregator <HDFS Input Directory> <HDFS Output Directory>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Force output to use a standard comma delimiter to align cleanly into target Spark frameworks [cite: 96, 111]
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Log Error Aggregator");
        job.setJarByClass(LogErrorAggregator.class);
        
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Accept comma-separated input files/directories directly
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
