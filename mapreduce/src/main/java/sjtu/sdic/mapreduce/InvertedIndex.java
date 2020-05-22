package sjtu.sdic.mapreduce;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/24.
 */
public class InvertedIndex {

    public static List<KeyValue> mapFunc(String file, String value) {
        // Your code here (Part V)
        List<KeyValue> res = new ArrayList<>();
        String regex = "[a-zA-Z0-9]+";
        Pattern p = Pattern.compile(regex);
        Matcher  ma = p.matcher(value);
        List<String> words = new ArrayList<>();
        while (ma.find()) {
            //String word = ma.group().toLowerCase();
            String word = ma.group();
            //System.out.println(word);
            words.add(word);
        }
        for (int i = 0; i < words.size(); i++) {
            KeyValue kv = new KeyValue(words.get(i), file);
            res.add(kv);
        }
        return res;
    }

    public static String reduceFunc(String key, String[] values) {
        //  Your code here (Part V)
        Set<String> fileSet = new TreeSet<>();
        for (String v : values) {
            if (!fileSet.contains(v)) {
                fileSet.add(v);
            }
        }
        int nFiles = fileSet.size();
        String res = nFiles + " ";
        List<String> fileList = new ArrayList<>(fileSet);
        Collections.sort(fileList);
        for (int i = 0; i < fileList.size(); i++) {
            if (i != fileList.size()-1) {
                res += fileList.get(i) + ",";
            } else {
                res += fileList.get(i);
            }
        }

        return res;
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("error: see usage comments in file");
        } else if (args[0].equals("master")) {
            Master mr;

            String src = args[2];
            File file = new File(".");
            String[] files = file.list(new WildcardFileFilter(src));
            if (args[1].equals("sequential")) {
                mr = Master.sequential("iiseq", files, 3, InvertedIndex::mapFunc, InvertedIndex::reduceFunc);
            } else {
                mr = Master.distributed("wcdis", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], InvertedIndex::mapFunc, InvertedIndex::reduceFunc, 100, null);
        }
    }
}
