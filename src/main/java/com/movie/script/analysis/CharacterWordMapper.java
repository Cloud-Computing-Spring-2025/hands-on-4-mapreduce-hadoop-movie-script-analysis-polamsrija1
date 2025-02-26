package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || !line.contains(":")) {
            return;
        }

        String[] parts = line.split(":", 2);
        if (parts.length < 2) {
            return;
        }

        String character = parts[0].trim();
        String dialogue = parts[1].trim();

        context.getCounter("Script Analysis", "Total Lines Processed").increment(1);
        context.getCounter("Script Analysis", "Total Characters Processed").increment(dialogue.length());

        StringTokenizer tokenizer = new StringTokenizer(dialogue);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
            if (!token.isEmpty()) {
                word.set(token);
                context.write(word, one);
                context.getCounter("Script Analysis", "Total Words Processed").increment(1);
            }
        }
    }
}