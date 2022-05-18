package com.xxx.magus.bean;

import java.io.Serializable;

/** 麦杰历史数据 */
public class Points implements Serializable {

  // 测点编码
  private String pointCode;
  // 测点值
  private String value;
  // 测点时间戳
  private long lTime;

  public long getlTime() {
    return lTime;
  }

  public void setlTime(long lTime) {
    this.lTime = lTime;
  }

  public String getPointCode() {
    return pointCode;
  }

  public void setPointCode(String pointCode) {
    this.pointCode = pointCode;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Points{"
        + "pointCode='"
        + pointCode
        + '\''
        + ", value='"
        + value
        + '\''
        + ", lTime="
        + lTime
        + '}';
  }
}
