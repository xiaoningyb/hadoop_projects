package com.apache.hadoop.yarn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class PartitionerClass implements Partitioner<Text, LongWritable>
    {
        public int getPartition(Text key, LongWritable value, int numPartitions)
        {
            if (numPartitions >= 2)//Reduce ∏ˆ ˝£¨≈–∂œ¡˜¡øªπ «¥Œ ˝µƒÕ≥º∆∑÷≈‰µΩ≤ªÕ¨µƒReduce
                if (key.toString().startsWith("logLevel::"))
                    return 0;
                else if(key.toString().startsWith("moduleName::"))
                    return 1;
                else return 0;
            else
                return 0;
        }
        public void configure(JobConf job){}    
}
