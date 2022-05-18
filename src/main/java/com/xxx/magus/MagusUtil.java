package com.xxx.magus;

import com.magus.net.OPConnect;
import com.magus.net.OPHisData;
import com.magus.net.OPNetConst;
import com.xxx.magus.bean.Points;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** crud magus数据 */
public class MagusUtil {


  /**
   * 查询单个测点的历史区间数据
   *
   * @param kks
   * @param from
   * @param to
   * @return
   */
  public static List<Points> getOPHisData(String kks, Date from, Date to) {

    List<Points> listData = new ArrayList<>();

    long count = 0;
    OPConnect conn = null;
    try {
      conn = new OPConnect("10.191.177.6", 8200, 60000, "sis", "openplant");
    } catch (Exception e) {
      e.printStackTrace();
    }

    // TODO 获取历史数据
    Map<String, OPHisData[]> pointHistorys = null;
    try {
      pointHistorys =
          conn.getPointHistorys(
              new String[] {"W3.SX." + kks}, from, to, OPNetConst.HISTORY_DATA_SAMPLE, 1);
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    for (Map.Entry<String, OPHisData[]> map : pointHistorys.entrySet()) {
      //      String kks = map.getKey();
      OPHisData[] hisData = map.getValue();
      int dataSize = hisData.length;

      //      System.out.println(dataSize+"data");

      // todo 处理数据
      for (int k = 0; k < dataSize; k++) {
        Points p = new Points();
        double av = hisData[k].getAV();
        long tm = hisData[k].getTime();
        p.setPointCode(kks);
        p.setValue(String.valueOf(av));
        p.setlTime(tm);
        listData.add(p);
      }
      count+=dataSize;
    }
    System.out.println(count);
    return listData;
  }
}
