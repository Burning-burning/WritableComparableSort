package com.atguigu.writableComparableSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public SortBean() {
    }
    public void set(long upFlow,long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }

    @Override
    public String toString() {
        return upFlow+" "+downFlow+" "+sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    public void readFields(DataInput in) throws IOException {

        upFlow = in.readLong();
        downFlow=in.readLong();
        sumFlow=in.readLong();
    }

    public int compareTo(SortBean o) {
        return Long.compare(o.sumFlow,this.sumFlow);
    }
}
