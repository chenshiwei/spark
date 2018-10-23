package csw.spark.es

import java.net.InetAddress
import java.util.Date

import com.alibaba.fastjson.JSONObject
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory

import scala.collection.JavaConverters._

/**
  * 作用:
  *
  * @author chensw
  * @since 2018/9/12
  */
case class ESConnection(host: String) {
  def connect(): TransportClient = {
    val settings: Settings = Settings.builder().build()
    val client: TransportClient = TransportClient.builder().settings(settings).build()
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300))
  }
}

object ESConnection extends App {
  val client = ESConnection("10.1.53.233").connect()
  val admin = client.admin().indices()
  admin.stats(new IndicesStatsRequest().all()).actionGet().getIndices.asScala.toList.foreach(x => println(x._1))
  println(client.prepareExists("spark2").execute().actionGet().exists())
  println(admin.prepareExists("spark2").execute().actionGet().isExists)
  //  val c = admin.prepareCreate("spark2")
  val mapping = XContentFactory.jsonBuilder()
    .startObject()
    .startObject("properties")
    .startObject("id").field("type", "long").endObject()
    .startObject("userName").field("type", "string").endObject()
    .startObject("birthDay").field("type", "date")
    .field("format", "strict_date_optional_time||epoch_millis").endObject()
    .endObject().endObject()
  println(mapping)

  //  c.addMapping("student",mapping)
  //  c.execute().actionGet()
  //  QueryBuilders.
  client.connectedNodes().asScala.toList.foreach(x => println(x.getHostAddress))
  val json =
    """
      |{
      |   "user":"csw",
      |   "postDate":"2018-09-13",
      |   "message":"trying out ES"
      |}
    """.stripMargin
  println(json)
  val json2 = new JSONObject()
  json2.put("user", "csw")
  json2.put("postDate", new Date())
  json2.put("message", "trying out ES!!!")
  println(json2)
  val source = XContentFactory.jsonBuilder().startObject().field("user", "csw")
    .field("postDate", new Date()).field("message", "trying out ES").endObject()
  println(source)
  val response = client.prepareIndex("twitter", "tweet", "3").setSource(source).execute().actionGet()
  println(s"${response.getIndex}::${response.getType}::${response.getId}::${response.getVersion}::${response.isCreated}")

  val response2: GetResponse = client.prepareGet("twitter", "tweet", "3")
    .setOperationThreaded(false).execute().actionGet()
  println(response2.getSourceAsString)

  val response3 = client.prepareDelete("twitter", "tweet", "1").execute().actionGet()
  println(s"${response3.getIndex}::${response3.getType}::${response3.getId}::${response3.getVersion}")

  val update = new UpdateRequest()
  update.id("3")
  update.`type`("tweet")
  update.index("twitter")
  update.doc(XContentFactory.jsonBuilder()
    .startObject().field("gender", "male").field("message", "hello").endObject())
  val response4 = client.update(update).get()
  println(s"${response4.getIndex}::${response4.getType}::${response4.getId}::${response4.getVersion}")


  val bulkRequest: BulkRequestBuilder = client.prepareBulk()
  bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
    .setSource(XContentFactory.jsonBuilder().startObject().field("user", "kimchy")
      .field("postDate", new Date()).field("message", "trying out Elasticsearch").endObject()))
  bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
    .setSource(XContentFactory.jsonBuilder.startObject.field("user", "kimchy")
      .field("postDate", new Date()).field("message", "another post").endObject))
  val response5 = bulkRequest.get
  println(response5.getHeaders)



  val response6 = client.prepareSearch("twitter").addFields("message", "_source").execute.actionGet
  for (hit <- response6.getHits.getHits) {
    println(hit.getId)
    if (hit.getFields.containsKey("message")) println("field.message: " + hit.getFields.get("message").getValue)
    println("source.message: " + hit.getSource.get("message"))
  }
}
