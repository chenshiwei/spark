package csw.spark.hadoop;


import uyun.whale.common.encryption.http.DecryptRequest;
import uyun.whale.common.encryption.http.HttpEncryptionWrapper;

import java.net.URLEncoder;

/**
 * 作用:
 *
 * @author chensw
 * @since 2019/12/5
 */
public class URLEncoderTest {
    public static void main(String[] args) throws Exception {
        System.out.println(URLEncoder.encode(">>>30ae7b38FDcb9ze8lpeZytpCVj0ouE/fGItEIdedK65YSgYlYzk=<<<","UTF-8"));
        DecryptRequest request = new DecryptRequest();
        request.setEncryptedText("123456");
        request.setUrl("http://10.1.11.153:7550/daemon/api/v2/encryption/decrypt");
        String plainText = HttpEncryptionWrapper.decrypt(request);
        System.out.println(plainText);

    }
}