package com.skangyam.hadoop.mapreduce.SetJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperSetJoin2 extends Mapper<LongWritable, Text, IntWritable, Text> 
{
	@Override
    protected void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException
    {
        String[] str = value.toString().split(",");
        IntWritable key1 = new IntWritable(Integer.parseInt(str[0]));
        context.write(key1, new Text(str[1]));
    }
}
