package csw.spark.es;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.Instant;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * 作用:
 *
 * @author chensw
 * @since 2019/9/3
 */
public class TransStructToMap implements UDF1<Row, Map<String, String>> {

    @Override
    public Map<String, String> call(Row row) throws Exception {
        java.util.Map<String, String> tagsMap = new HashMap<>();
        StructType structType = row.schema();
        StructField[] fields = structType.fields();
        for (int n = 0; n < fields.length; n++) {
            StructField field = fields[n];
            tagsMap.put(field.name(), transObjectToString(row.get(n)));
        }
        return tagsMap;
    }

    public static String transObjectToString(Object obj) {
        if (obj == null) {
            return "";
        } else if (obj instanceof String) {
            return (String)obj;
        } else if (obj instanceof Timestamp) {
            Timestamp ts = (Timestamp)obj;
            return String.valueOf(ts.getTime());
        } else if (obj instanceof Instant) {
            Instant ts = (Instant)obj;
            long millis = ts.getMillis();
            return String.valueOf(millis);
        } else {
            return obj.toString();
        }
    }
}