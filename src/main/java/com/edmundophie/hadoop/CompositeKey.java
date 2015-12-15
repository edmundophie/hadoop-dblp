package com.edmundophie.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by edmundophie on 12/15/15.
 */
public class CompositeKey implements WritableComparable{
    private String author;
    private int publicationQty;

    public CompositeKey() {}

    public CompositeKey(String author, int publicationQty) {
        this.author = author;
        this.publicationQty = publicationQty;
    }

    public int compareTo(Object k) {
        CompositeKey o = (CompositeKey) k;
        int result = author.compareTo(o.author);
        if(result==0)
            result = publicationQty<o.publicationQty?-1:(publicationQty==o.publicationQty?0:1);
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, author);
        dataOutput.writeInt(publicationQty);
    }

    public void readFields(DataInput dataInput) throws IOException {
        author = WritableUtils.readString(dataInput);
        publicationQty = dataInput.readInt();
    }

    @Override
    public String toString() {
        return author;
    }

    public String getAuthor() {return this.author;}
    public int getPublicationQty() {return this.publicationQty;}
    public void setAuthor(String author) {this.author = author;}
    public void setPublicationQty(int publicationQty) {this.publicationQty = publicationQty;}
}
