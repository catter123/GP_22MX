package Tags

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * @author catter
  * @date 2019/8/24 11:16
  */
object HttpUtil {

  def get(url:String): String ={

    val client = HttpClients.createDefault()
    val get = new HttpGet(url)
    //发送请求
    val response: CloseableHttpResponse = client.execute(get)
    //返回结果

    EntityUtils.toString(response.getEntity,"utf-8")


  }


}
