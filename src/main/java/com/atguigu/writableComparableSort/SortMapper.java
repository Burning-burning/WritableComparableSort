package com.atguigu.writableComparableSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean,Text> {
    private SortBean sortBean = new SortBean();
    private Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String phone = fields[0];
        this.text.set(phone);

        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        Long sumFlow = Long.parseLong(fields[3]);
        this.sortBean.set(upFlow,downFlow);
        context.write(sortBean,text);
    }
}
