package com.alibaba.streamcompute.data;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.streamcompute.impl.HBaseServiceImpl;
import com.alibaba.streamcompute.service.StorageService;
import com.alibaba.streamcompute.tools.HBaseUtil;
import org.apache.hadoop.hbase.client.*;
import scala.collection.mutable.StringBuilder;

import java.io.*;
import java.util.*;

import static com.alibaba.streamcompute.tools.HBaseUtil.*;
import static com.alibaba.streamcompute.tools.Util.getSample;

public class GenerateTrainData {

    private static BufferedWriter bw;
    private static StorageService storageService;

    public static void main(String[] args) throws IOException {

        File file = new File("/Users/zhangpeng/venv/work/train.data");
        FileWriter fw = new FileWriter(file);
        bw = new BufferedWriter(fw);
        storageService = new HBaseServiceImpl();

        Connection connection = HBaseUtil.getHbaseConnection();
        List<Result> results = ( List<Result>) storageService.scanData("click", new ArrayList<>());

        try {

            for (Result result: results) {
                Map<String, String> row = getRow(result);

                if (!row.containsKey("date")) {
                    row.put("date", "2019-09-05");
                }

                String itemId = row.get("item_id");
                String userId = row.get("user_id");

                ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
                userFilterInfos.add(getFilterInfo("user_id", userId, "cf"));

                ArrayList<Map<String, String>> itemFilterInfos = new ArrayList<>();
                itemFilterInfos.add(getFilterInfo("item_id", itemId, "cf"));

                Map<String, String> userInfo =
                        getRow(((List<Result>) storageService.scanData("user", userFilterInfos)).get(0));
                Map<String, String> itemInfo =
                        getRow(((List<Result>) storageService.scanData("item", itemFilterInfos)).get(0));

                Map<String, String> sample = getSample(userInfo, itemInfo);
                sample.put("label", row.get("flag"));
                sample.put("date", row.get("date"));

                long time = System.currentTimeMillis();
                String feature_string = JSONObject.toJSONString(sample);
                String rowkey = userId + "-" + itemId + "-" + time;
                storageService.writeData("train_data", "com/alibaba/streamcompute/data", rowkey, feature_string);
//                writeToCSV(sample);

                StringBuilder keys = new StringBuilder();
                for (String key : sample.keySet()) {
                    keys.append("\"");
                    keys.append(key);
                    keys.append("\",");
                }
                System.out.println(keys);
            }
            bw.flush();
            fw.close();
            bw.close();
            connection.close();
        } catch (Exception ignore) {
        }

    }

    public static void writeToCSV(Map<String, String> data) throws IOException {
        String value = "";
        for (Map.Entry entry : data.entrySet()) {
            value = value + entry.getValue() + ",";
        }
        String result = value.substring(0, value.length() - 1) + "\r\n";
        bw.write(result);
    }


}
