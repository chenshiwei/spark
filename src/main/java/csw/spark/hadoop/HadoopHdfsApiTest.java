package csw.spark.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;


/**
 *  * 下载hdfs上的指定文件到本地
 *  * @company 源辰信息
 *  * @author navy
 *  
 */
public class HadoopHdfsApiTest {
//    private static Logger log = Logger.getLogger(HadoopHdfsApiTest.class); // 创建日志记录器


    @SuppressWarnings("resource")
    public static void main(String[] args) {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration(); // 加载配置文件
            URI uri = new URI("hdfs://10.1.11.154:19000"); // 连接资源位置
            fs = FileSystem.get(uri, conf, "root"); // 创建文件系统实例对象
            FileStatus[] files = fs.listStatus(new Path("/uyun/databank/spark/evolution/pipeline"));  // 列出文件
            String path = "/uyun/databank/spark/custpy/pipeline-ml-operator-5.0.0-SNAPSHOT.jar";
//            JarFile jarFile = new JarFile(new File(path));
//            URL url = new URL("hdfs://127.0.0.1:19000" + path);
//            ClassLoader loader = new URLClassLoader(new URL[]{url});

            for (FileStatus f : files) {
                System.out.println("\t" + f.getPath());
            }

            Path dstPath =  new Path("/uyun/databank/spark/evolution/pipeline/model2");
            fs.delete(dstPath, true);
//            FSDataOutputStream outputStream = fs.create(dstPath);
//            if (!fs.exists(dstPath.getParent())) {
//                fs.mkdirs(dstPath.getParent());
//            }
//            outputStream.write("KNKIN".getBytes());
//            outputStream.flush();
//
//            fs.copyToLocalFile(false,p,dst,true);
//
//            System.out.print("下载成功....");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("hdfs操作失败!!!");
        }
    }
}
