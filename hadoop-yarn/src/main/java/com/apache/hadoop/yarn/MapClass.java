package com.apache.hadoop.yarn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase 
        implements Mapper<LongWritable, Text, Text, LongWritable> 
    {
        public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException
        {    
            String line = value.toString();//√ª”–≈‰÷√RecordReader£¨À˘“‘ƒ¨»œ≤…”√lineµƒ µœ÷£¨keyæÕ «––∫≈£¨valueæÕ «––ƒ⁄»›
            System.out.println("line=="+line);
            if (line == null || line.equals(""))
                return;
            String[] words = line.split(" ");
            if (words == null || words.length < 3)
                return;
            String logLevel = words[1];
            String moduleName = words[2];
            LongWritable recbytes = new LongWritable(1);
            Text record = new Text();
            record.set(new StringBuffer("logLevel::").append(logLevel).toString());
            reporter.progress();
            output.collect(record, recbytes);// ‰≥ˆ»’÷æº∂±Õ≥º∆Ω·π˚£¨Õ®π˝logLevel::◊˜Œ™«∞◊∫¿¥±Í æ°£
            record.clear();
            record.set(new StringBuffer("moduleName::").append(moduleName).toString());
            System.out.println("output key=="+record.toString());
            output.collect(record, new LongWritable(1));// ‰≥ˆƒ£øÈ√˚µƒÕ≥º∆Ω·π˚£¨Õ®π˝moduleName::◊˜Œ™«∞◊∫¿¥±Í æ
        }    
    }
