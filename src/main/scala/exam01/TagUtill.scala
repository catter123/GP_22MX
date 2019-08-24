package exam01



import Tags.HttpUtil
import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * @author catter
  * @date 2019/8/24 15:16
  */
object TagUtill {

  def getbuccessTags(Str:String): List[(String,String)]= {

//    val list = collection.mutable.ListBuffer[String]()
    var lists =List[(String,String)]()



    val list = collection.mutable.ListBuffer[String]()

    //获取json穿，解析json
    val jsonstr: JSONObject = JSON.parseObject(Str)

    val stutus = jsonstr.getIntValue("status")
    if(stutus==0){
      return lists
    }

    //判断key的value不能为空
    val regeocode = jsonstr.getJSONObject("regeocode")
    if(regeocode==null || regeocode.keySet().isEmpty) {
      return lists
    }

    val pois = regeocode.getJSONArray("pois")
    if(pois==null|| pois.isEmpty){
      return lists
    }
    for (item<-pois.toArray()){
      val json = item.asInstanceOf[JSONObject]
      val businessarea = json.getString("businessarea")
      val types = json.getString("type")

      lists:+=(businessarea,types)

    }

    lists



  }






}
