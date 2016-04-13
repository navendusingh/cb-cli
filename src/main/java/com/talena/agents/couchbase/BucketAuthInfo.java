package com.talena.agents.couchbase;

public class BucketAuthInfo {
  private String bucketName;
  private String password;

  public BucketAuthInfo() {
    this.bucketName = "";
    this.password = "";
  }

  public BucketAuthInfo(final String bucketName, final String password) {
    this.bucketName = bucketName;
    this.password = password;
  }

  public BucketAuthInfo(final BucketAuthInfo bucket) {
    this.bucketName = bucket.bucketName;
    this.password = bucket.password;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public String toString() {
    return "Bucket [bucketName=" + bucketName + ", password=" + password + "]";
  }
}
