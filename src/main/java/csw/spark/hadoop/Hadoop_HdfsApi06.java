package csw.spark.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;


/**
  * 下载hdfs上的指定文件到本地
  * @company 源辰信息
  * @author navy
  */
public class Hadoop_HdfsApi06 {
//    private static Logger log = Logger.getLogger(Hadoop_HdfsApi06.class); // 创建日志记录器


    @SuppressWarnings("resource")
    public static void main(String[] args) {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration(); // 加载配置文件
            URI uri = new URI("hdfs://10.1.62.12:19000"); // 连接资源位置
            fs = FileSystem.get(uri, conf ,"uyun"); // 创建文件系统实例对象
            Path p= new Path("/uyun/databank/spark/cert/keyStore.txt_1575553586460");
            FileStatus[] files = fs.listStatus(new Path("/uyun/databank/spark/cert/"));  // 列出文件

            for (FileStatus f : files) {
                System.out.println("\t" + f.getPath().getName());
            }
            Path dst=new Path("E:/tmp/");

            fs.copyToLocalFile(false,p,dst,true);

            System.out.print("下载成功....");

        } catch (Exception e) {
            System.out.print("hdfs操作失败!!!");
        }
    }
}
