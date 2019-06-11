package assignment2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    private static Comparator<Pair> pairComparator = new Comparator<Pair>() {
        public int compare(Pair a, Pair b) {
            return (a.count != b.count) ? a.count - b.count : b.word.compareTo(a.word);
        }
    };

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private Text word = new Text();

        private Map<String, Integer> map = new HashMap<String, Integer>();
        private PriorityQueue<Pair> minHeap = new PriorityQueue<Pair>(100, pairComparator);;

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) {
            if (key.toString().length() > 3) {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                if (!map.containsKey(key.toString()))
                    map.put(key.toString(), sum);
                else
                    map.put(key.toString(), map.get(key.toString()) + sum);
            }
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException {
            Pair p;
            for (Map.Entry<String, Integer> entry: map.entrySet()) {
                p = new Pair(entry.getKey(), entry.getValue());
                minHeap.add(p);
                // keep size of min heap 100
                if (minHeap.size() > 100) minHeap.poll();
            }
            // output
            while (!minHeap.isEmpty()) {
                p = minHeap.poll();
                word.set(p.word);
                result.set(p.count);
                context.write(word, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // split input size
        //conf.set("mapreduce.input.fileinputformat.split.maxsize", "100");
        //conf.set("mapreduce.input.fileinputformat.split.minsize", "100");
        // set number of reducers
        //job.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean sysCompleted = job.waitForCompletion(true);
        // output execution time
        long end = System.currentTimeMillis();
        System.out.printf("Execution Time: %d s\n", (end - start) / 1000);
        System.exit(sysCompleted ? 0 : 1);
    }
}