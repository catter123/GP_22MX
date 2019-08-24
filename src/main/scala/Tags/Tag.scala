package Tags

/**
  * @author catter
  * @date 2019/8/23 11:14
  *
  *      打标接口
  */
trait Tag {

  def makeAdTags(args:Any*):List[(String,Int)]{}
  def makeAPTags(args:Any*):List[(String,Int)]{}
  def makeAndroidTags(args:Any*):List[(String,Int)]{}
  def makenetworkTags(args:Any*):List[(String,Int)]{}
  def makeispidTags(args:Any*):List[(String,Int)]{}
  def makeKeyTags(args:Any*):List[(String,Int)]{}
  def makeLocationTags(args:Any*):List[(String,Int)]{}
  def makebuccessTags(args:Any*):List[(String, Int)]{}
//  def makeContextTags(args:Any*):List[(String,Int)]{}
  def makeAppTags(args:Any*):List[(String,Int)]{}




}
