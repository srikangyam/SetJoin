package com.skangyam.hadoop.mapreduce.SetJoin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerSetJoin extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
	          throws IOException, InterruptedException{
		HashSet<String> hs = new HashSet<String>();
		for (Text info : values){
			String str = info.toString();
			hs.add(str);
		}
		
		Iterator<String> hsITR = hs.iterator();
		int count = 0;
		String oValue = "";
		while (hsITR.hasNext()){
			oValue += hsITR.next();
			count++;
			if (count < hs.size()){
				oValue += ",";
			}
		}
		
		context.write(key, new Text(oValue));
		
	}

}
