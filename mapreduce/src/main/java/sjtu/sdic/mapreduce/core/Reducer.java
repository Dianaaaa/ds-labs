package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import com.alibaba.fastjson.JSON;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class Reducer {

    /**
     * 
     * 	doReduce manages one reduce task: it should read the intermediate
     * 	files for the task, sort the intermediate key/value pairs by key,
     * 	call the user-defined reduce function {@code reduceF} for each key,
     * 	and write reduceF's output to disk.
     * 	
     * 	You'll need to read one intermediate file from each map task;
     * 	{@code reduceName(jobName, m, reduceTask)} yields the file
     * 	name from map task m.
     *
     * 	Your {@code doMap()} encoded the key/value pairs in the intermediate
     * 	files, so you will need to decode them. If you used JSON, you can refer
     * 	to related docs to know how to decode.
     * 	
     *  In the original paper, sorting is optional but helpful. Here you are
     *  also required to do sorting. Lib is allowed.
     * 	
     * 	{@code reduceF()} is the application's reduce function. You should
     * 	call it once per distinct key, with a slice of all the values
     * 	for that key. {@code reduceF()} returns the reduced value for that
     * 	key.
     * 	
     * 	You should write the reduce output as JSON encoded KeyValue
     * 	objects to the file named outFile. We require you to use JSON
     * 	because that is what the merger than combines the output
     * 	from all the reduce tasks expects. There is nothing special about
     * 	JSON -- it is just the marshalling format we chose to use.
     * 	
     * 	Your code here (Part I).
     * 	
     * 	
     * @param jobName the name of the whole MapReduce job
     * @param reduceTask which reduce task this is
     * @param outFile write the output here
     * @param nMap the number of map tasks that were run ("M" in the paper)
     * @param reduceF user-defined reduce function
     */
    public static void doReduce(String jobName, int reduceTask, String outFile, int nMap, ReduceFunc reduceF) {
        List<KeyValue> kvs = new ArrayList<>();
        for (int i = 0; i < nMap; i++) {
            String fileName = Utils.reduceName(jobName, i, reduceTask);
            //System.out.println("ReduceName: " + fileName);
            String content = null;
            try {
                File file = new File(fileName);
                FileInputStream inputStream = new FileInputStream(file);
                int length = inputStream.available();
                byte bytes[] = new byte[length];
                inputStream.read(bytes);
                inputStream.close();
                content =new String(bytes, StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }
            List<String> strs = Arrays.asList(content.split("\n"));
            for (int j = 0; j < strs.size(); j++) {
                //System.out.println(strs.size());
//                System.out.println("AM i mad?");
                KeyValue kv = JSON.parseObject(strs.get(j), KeyValue.class);
                //System.out.println(kv);
                kvs.add(kv);
            }
        }

        kvs.sort((a,b) -> (a.key.compareTo(b.key)));
        Map<String, List<String>> kvsMap = new TreeMap<>();
        for (int i = 0; i < kvs.size(); i++) {
            if (kvsMap.get(kvs.get(i).key) == null) {
                List<String> values = new ArrayList<String>();
                values.add(kvs.get(i).value);
                kvsMap.put(kvs.get(i).key, values);
            } else {
                kvsMap.get(kvs.get(i).key).add(kvs.get(i).value);
            }
        }

        String mergeFileName = Utils.mergeName(jobName, reduceTask);
        File mergeFile = new File(mergeFileName);
        //System.out.println("MergeFileName: " + mergeFile);
        FileWriter writer = null;
        try {
            if (!mergeFile.exists()) {
                mergeFile.createNewFile();
            }
            writer = new FileWriter(mergeFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, Object> resMap = new TreeMap<>();
        for (String key : kvsMap.keySet()) {
            List<String> listV = kvsMap.get(key);
            String[] values = listV.toArray(new String[listV.size()]);
            String res = reduceF.reduce(key, values);
            resMap.put(key, res);
        }
        try {
            JSONObject json = new JSONObject(resMap);
            writer.append(json.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
