package me.xuling.geek.bigdata.week01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author jack
 * @since 2022/3/9
 **/
public class PhoneData implements WritableComparable<PhoneData> {
    private String phoneNumber;
    private long downFlow;
    private long upFlow;
    private long sumFlow;

    public PhoneData() {}

    public PhoneData(String phoneNumber, long upFlow, long downFlow) {
        super();
        this.phoneNumber = phoneNumber;
        this.downFlow = downFlow;
        this.upFlow = upFlow;
        this.sumFlow = downFlow + upFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeUTF(phoneNumber);
       dataOutput.writeLong(upFlow);
       dataOutput.writeLong(downFlow);
       dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneNumber = dataInput.readUTF();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    @Override
    public int compareTo(PhoneData o) {
        return Long.compare(o.sumFlow, this.sumFlow);
    }
}
