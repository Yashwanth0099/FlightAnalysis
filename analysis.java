package analysis;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Analysis {

    // Mapper for flight schedules
    public static class MapperA extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");
            if (row.length > 15) { // Ensure row has enough columns
                try {
                    String carrier = row[8].trim();
                    String delay1 = row[14].trim();
                    String delay2 = row[15].trim();

                    if (delay1.matches("-?\\d+") && delay2.matches("-?\\d+")) {
                        int delay = Integer.parseInt(delay1) + Integer.parseInt(delay2);
                        if (delay <= 10) {
                            context.write(new Text(carrier), new IntWritable(1)); // On schedule
                        } else {
                            context.write(new Text(carrier), new IntWritable(0));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error processing row: " + value.toString());
                }
            }
        }
    }

    // Reducer for flight schedules
    public static class ReducerA extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private List<Flight> top3 = new ArrayList<>();
        private List<Flight> bot3 = new ArrayList<>();

        class Flight implements Comparable<Flight> {
            String carrier;
            double prob;

            public Flight(String carrier, double prob) {
                this.carrier = carrier;
                this.prob = prob;
            }

            @Override
            public int compareTo(Flight f) {
                return Double.compare(f.prob, this.prob); // Descending order
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int onTime = 0;

            for (IntWritable val : values) {
                count++;
                if (val.get() == 1) {
                    onTime++;
                }
            }

            if (count > 0) {
                double finProb = ((double) onTime) / count;
                Flight tempFlight = new Flight(key.toString(), finProb);
                updateTopAndBottom(tempFlight);
            }
        }

        private void updateTopAndBottom(Flight f) {
            top3.add(f);
            bot3.add(f);

            // Maintain top 3 list
            Collections.sort(top3);
            if (top3.size() > 3) {
                top3.remove(top3.size() - 1);
            }

            // Maintain bottom 3 list
            Collections.sort(bot3, Comparator.comparingDouble(a -> a.prob)); // Ascending order
            if (bot3.size() > 3) {
                bot3.remove(bot3.size() - 1);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Top 3 airlines with the highest on-schedule probability:"), null);
            for (Flight f : top3) {
                context.write(new Text(f.carrier), new DoubleWritable(f.prob));
            }

            context.write(new Text("Top 3 airlines with the lowest on-schedule probability:"), null);
            for (Flight f : bot3) {
                context.write(new Text(f.carrier), new DoubleWritable(f.prob));
            }
        }
    }

    // Mapper for Taxi Time
    public static class MapperB extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");
            if (row.length > 20) { // Ensure row has enough columns
                try {
                    String origin = row[16].trim();
                    String dest = row[17].trim();
                    String taxiIn = row[19].trim();
                    String taxiOut = row[20].trim();

                    if (taxiIn.matches("\\d+") && taxiOut.matches("\\d+")) {
                        context.write(new Text(dest), new IntWritable(Integer.parseInt(taxiIn)));
                        context.write(new Text(origin), new IntWritable(Integer.parseInt(taxiOut)));
                    }
                } catch (Exception e) {
                    System.err.println("Error processing row: " + value.toString());
                }
            }
        }
    }

    // Reducer for Taxi Time
    public static class ReducerB extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private List<Taxi> long3 = new ArrayList<>();
        private List<Taxi> short3 = new ArrayList<>();

        class Taxi implements Comparable<Taxi> {
            String airport;
            double time;

            public Taxi(String airport, double time) {
                this.airport = airport;
                this.time = time;
            }

            @Override
            public int compareTo(Taxi t) {
                return Double.compare(t.time, this.time); // Descending order
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int totalTime = 0;

            for (IntWritable val : values) {
                count++;
                totalTime += val.get();
            }

            if (count > 0) {
                double avgTime = (double) totalTime / count;
                Taxi tempTaxi = new Taxi(key.toString(), avgTime);
                updateLongAndShort(tempTaxi);
            }
        }

        private void updateLongAndShort(Taxi t) {
            long3.add(t);
            short3.add(t);

            // Maintain top 3 longest times
            Collections.sort(long3);
            if (long3.size() > 3) {
                long3.remove(long3.size() - 1);
            }

            // Maintain top 3 shortest times
            Collections.sort(short3, Comparator.comparingDouble(a -> a.time)); // Ascending order
            if (short3.size() > 3) {
                short3.remove(short3.size() - 1);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Top 3 airports with the longest average taxi time:"), null);
            for (Taxi t : long3) {
                context.write(new Text(t.airport), new DoubleWritable(t.time));
            }

            context.write(new Text("Top 3 airports with the shortest average taxi time:"), null);
            for (Taxi t : short3) {
                context.write(new Text(t.airport), new DoubleWritable(t.time));
            }
        }
    }

    // Mapper for Flight Cancellation Reasons
    public static class MapperC extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");
            if (row.length > 22) { // Ensure row has enough columns
                String code = row[22].trim();
                if (!code.equals("CancellationCode") && !code.equals("NA") && !code.isEmpty()) {
                    context.write(new Text(code), new IntWritable(1));
                }
            }
        }
    }

    // Reducer for Flight Cancellation Reasons
    public static class ReducerC extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text mostCommonCode = new Text();
        private int maxCount = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            if (count > maxCount) {
                maxCount = count;
                mostCommonCode.set(key);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("The most common reason for flight cancellations:"), null);
            context.write(mostCommonCode, new IntWritable(maxCount));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Job 1: Flight schedule analysis
        Job job1 = Job.getInstance(conf, "Flight Schedule Analysis");
        job1.setJarByClass(Analysis.class);
        job1.setMapperClass(MapperA.class);
        job1.setReducerClass(ReducerA.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path outputPath1 = new Path(args[1] + "_schedule");
        if (fs.exists(outputPath1)) fs.delete(outputPath1, true);
        FileOutputFormat.setOutputPath(job1, outputPath1);
        if (!job1.waitForCompletion(true)) System.exit(1);

        // Job 2: Taxi time analysis
        Job job2 = Job.getInstance(conf, "Taxi Time Analysis");
        job2.setJarByClass(Analysis.class);
        job2.setMapperClass(MapperB.class);
        job2.setReducerClass(ReducerB.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        Path outputPath2 = new Path(args[1] + "_taxi");
        if (fs.exists(outputPath2)) fs.delete(outputPath2, true);
        FileOutputFormat.setOutputPath(job2, outputPath2);
        if (!job2.waitForCompletion(true)) System.exit(1);

        // Job 3: Flight cancellation reasons
        Job job3 = Job.getInstance(conf, "Flight Cancellation Reasons");
        job3.setJarByClass(Analysis.class);
        job3.setMapperClass(MapperC.class);
        job3.setReducerClass(ReducerC.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        Path outputPath3 = new Path(args[1] + "_cancel");
        if (fs.exists(outputPath3)) fs.delete(outputPath3, true);
        FileOutputFormat.setOutputPath(job3, outputPath3);
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
