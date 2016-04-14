package com.talena.agents.couchbase;

public class AuthInfo {
  private String name;
  private String password;

  public AuthInfo() {
    this.name = "";
    this.password = "";
  }

  public AuthInfo(final String name, final String password) {
    this.name = name;
    this.password = password;
  }

  public AuthInfo(final AuthInfo bucket) {
    this.name = bucket.name;
    this.password = bucket.password;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public String toString() {
    return "AuthInfo [name=" + name + ", password=" + password + "]";
  }
}
