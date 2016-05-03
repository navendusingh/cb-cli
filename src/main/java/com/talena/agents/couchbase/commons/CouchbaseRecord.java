package com.talena.agents.couchbase.commons;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CouchbaseRecord implements Writable {

  public enum Type {
    INSERT, UPDATE, DELETE, EVENT
  }

  protected Type recType;
  
  public CouchbaseRecord(Type type) {
    this.recType = type;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    // TODO: Do we need to write out type?   
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO: Do we need to read type?
  }

}
