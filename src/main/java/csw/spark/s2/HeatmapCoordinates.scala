package csw.spark.s2

import scala.beans.BeanProperty

/**
 * @author chensw
 * @since at 2016-10-19下午3:04:46
 */
class HeatmapCoordinates extends Serializable {

  @BeanProperty var scheduler_timestamp: Long = _
  @BeanProperty var appid: String = _
  @BeanProperty var platform: String = _
  @BeanProperty var url: String = _
  @BeanProperty var resolution: Option[String] = _
  @BeanProperty var identify_id: String = _
  @BeanProperty var user_id: String = _
  @BeanProperty var session_id: String = _
  @BeanProperty var page_id: String = _
  @BeanProperty var timestamp: Long = _
  @BeanProperty var pixel_coordinates: Option[String] = _
  @BeanProperty var window_size: Option[String] = _

}


