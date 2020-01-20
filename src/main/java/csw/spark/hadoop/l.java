package csw.spark.hadoop;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 作用:
 *
 * @author chensw
 * @since 2019/12/5
 */
public class l {
    public static void main(String[] args){
        String b = "hdfs://10.1.62.12:19000/uyun/databank/spark/cert/data1.txt_1575527567894";
        Pattern p = Pattern.compile("(hdfs://\\d+\\.\\d+\\.\\d+\\.\\d+\\:\\d+)()");
        Matcher m = p.matcher(b);
        while(m.find()) {

            System.out.println("ip:"+m.group(1));
            System.out.println("port:"+m.group(2));
        }

    }
}