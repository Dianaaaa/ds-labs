# Lab 4: MapReduce

516072910066 钱星月

## Part I: Map/Reduce input and output

这一部分是实现Mapper.java和Reducer.java中的doMap()和doRedoce()函数。

### 实现思路

- doMap()

  伪Java代码如下：

  ```java
  String content = ReadFile(infile);		//先读取input文件
  List<KeyValue> keyValueList = mapF.map(infile, content); //调用map函数，得到很多keyValue对；
  for (int i=0; i<nReduce; i++) 
  {
  	String reduceFile = Utils.reduceName(jobName, mapTask, i); //根据规则得到reduce文件的name
  	for KeyValue keyValue : keyValueList 
    {
  		if (hashCode(keyValue.key) % nReduce == i) //keyValue对通过key映射到reduce文件
      {
        String writeContent = toJSONString(keyValue) + "\n" //每行用换行符隔开
        WriteFile(reduceFile, writeContent); //将keyValue转成JSON格式写入reduce文件
      }
  	}
  }
  ```

  以上的ReadFile()和WriteFile()是读文件和写文件过程抽象出来的函数。

  总结：

  	1.	读取input文件
   	2.	调用mapF，得到KeyValue的List
   	3.	通过hash把Key映射到相应的reduceFile中
   	4.	KeyValue对写入reduceFile

- doReduce()

  伪Java代码如下：

  ```java
  for (int i=0; i<nMap; i++)
  {
  	String reduceFile = Utils.reduceName(jobName, i, reduceTask); //按规则得到要进行reduce的文件名
    String content = ReadFile(reduceFile); //读取整个文件内容
    String[] stringList = content.split("\n") //将文本内容分割成一行行，得到一个List
      
    List<KeyValue> keyValueList;
    for each str in stringList 
    {
      KeyValue keyValue = parseJSON(str); //将每一行从JSON格式解析成keyValue
    	keyValueList.add(keyValue);	//加入keyValueList
    }
     
    Map<String, String[]> map = toMap(keyValueList); //这一步代表将keyValueList中相同key的内容合并后，得到的map。
    map.sort() //对map排序
      
    Map<String, Object> finalMap;	//key--最终结果 的map
    for (String key : map.keySet()) {
      String res = reduceF.reduce(key, map[key]);	//将map中的每一对放入reduce()函数中计算，得到结果
      finalMap.add(key, res);	//将key-res对放入finalMap
    }
    
    String mergeFile = Utils.mergeName(jobName, reduceTask); //按规则得到merge的文件名
    WriteFile(mergeFile, toJSONString(finalMap)); //将finalMap以JSON格式写入merge文件
  }
  ```

  以上的函数均为简化后的函数。

  总结：

  	1. 把reduce文件中的内容转成KeyValue
   	2. 把KeyValue相同的Key对应的内容合并成一个String[]，并排序
   	3. 把上一步得到的Key和String[] 放入reduceFunc中计算得到结果res
   	4. 最后将一对对key和res写入mergeFile中。

## Part II: Single-worker word count

- mapFunc()

  Java代码如下（不是伪Java代码了）:

  ```java
  public static List<KeyValue> mapFunc(String file, String value) {
          // Your code here (Part II)
          List<KeyValue> res = new ArrayList<>();
          String regex = "[a-zA-Z0-9]+";
          Pattern p = Pattern.compile(regex);
          Matcher  ma = p.matcher(value);
          List<String> words = new ArrayList<>();
          while (ma.find()) {
              String word = ma.group();\
              words.add(word);
          }
          for (int i = 0; i < words.size(); i++) {
              KeyValue kv = new KeyValue(words.get(i), "");
              res.add(kv);
          }
          return res;
      }
  ```

  1. 使用正则表达式，从文件中获取单词，得到一个word List
  2. word List中每一个word，组成一个 [word, ""] 的keyValue对
  3. 得到keyValue List

- reduceFunc()

  Java代码如下：

  ```java
  public static String reduceFunc(String key, String[] values) {
          // Your code here (Part II)
          String res = String.valueOf(values.length);
          return res;
      }
  ```

  在map中，keyValue对的value是个空字符串，要计算Key（也就是word）出现的次数，只需知道它对应的values的个数即可。

## Part III: Distributing MapReduce tasks

简化后的Java代码（去除避免warning的try catch语句后）如下：

```java
CountDownLatch count = new CountDownLatch(nTasks);
for (int i = 0; i < nTasks; i++) {
    DoTaskArgs args = new DoTaskArgs(jobName, mapFiles[i], phase, i, nOther); //构造args
    new Thread(() -> {
        String woker = registerChan.read();	//从Channel中获取worker
        Call.getWorkerRpcService(woker).doTask(args);
        registerChan.write(woker);	//把worker放回Channel
       	count.countDown();
    }).start();	//多线程并行
}

count.await(); // 等待所有线程完成工作
```

1. 分离出nTasks个线程，每个线程从Channel中获取代表worker的字符串，发送RPC调用，完成后调用CountDownLatch的countDown()
2. 在nTasks个线程均调用了countDown() （即完成工作）前，主线程阻塞在了count.await()。等到所有工作完成才 接着运行。

## Part IV: Handling worker failures

在Part III的基础上加入 SofaRpcException的异常处理即可。

简化后的Java代码如下：

```java
CountDownLatch count = new CountDownLatch(nTasks);
for (int i = 0; i < nTasks; i++) {
    DoTaskArgs args = new DoTaskArgs(jobName, mapFiles[i], phase, i, nOther); //构造args
    new Thread(() -> {
        String woker = registerChan.read();	//从Channel中获取worker
      	/*****较Part III有改动的地方*****/
        Boolean flag = Boolean.TRUE;
        do {
            try {
                Call.getWorkerRpcService(woker).doTask(args);
                flag = Boolean.TRUE;
            } catch (SofaRpcException e) { // 处理SofaRpcException异常
                registerChan.write(woker);
                woker = registerChan.read();
                flag = Boolean.FALSE;
            }
        } while (!flag); //如果RPC一直失败，将不断重新尝试
      	/****************************/
        registerChan.write(woker);	//把worker放回Channel
       	count.countDown();
    }).start();	//多线程并行
}

count.await(); // 等待所有线程完成工作
```

## Part V: Inverted index generation (optional, as bonus)

与Word Count大同小异：

	1.	在MapFunc中：生成 [word, fileName] 的KeyValue对
 	2.	在reduceFunc中：res的内容变为 “[文件数] [用‘，’隔开的文件名]” 的格式

代码如下：

```java
public static List<KeyValue> mapFunc(String file, String value) {
        // Your code here (Part V)
        List<KeyValue> res = new ArrayList<>();
        String regex = "[a-zA-Z0-9]+";
        Pattern p = Pattern.compile(regex);
        Matcher  ma = p.matcher(value);
        List<String> words = new ArrayList<>();
        while (ma.find()) {
            String word = ma.group();
            words.add(word);
        }
        for (int i = 0; i < words.size(); i++) {
            KeyValue kv = new KeyValue(words.get(i), file); //MapFunc与wordCount只有这一行不同
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
      
      	// 构造res字符串
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
```



