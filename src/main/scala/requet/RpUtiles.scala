package requet

/**
  * @author catter
  * @date 2019/8/21 10:46
  */
object RpUtiles {

    //处理请求数
    def request(requestmode:Int,processnode:Int):List[Double]={

        if (requestmode==1&&processnode>=3){
          List[Double](1,1,1)
        }else if(requestmode==1&&processnode>=2){
          List[Double](1,1,0)
        }else if(requestmode==1&&processnode>=1){
          List[Double](1,0,0)
        }else{
          List[Double](0,0,0)
        }

    }

    //处理展示点击
    def click(requestmode:Int,iseffective:Int):List[Double]={

      if (requestmode==2&&iseffective==1){
       List[Double](1,0)
      }else if (requestmode==3&&iseffective==1){
        List[Double](0,1)
      }else{
        List[Double](0,0)
      }

    }

    //处理竞价
    def Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Double,adordeerid:Double,winprice:Double,adpayment:Double):List[Double]={

      var num4=0.0
      var num5=0.0
      var num8=0.0
      var num9=0.0
      if(iseffective==1&&isbilling==1&&isbid==1){
        num4+=1
      }
      if(iseffective==1&&isbilling==1&&isbid==1&&iswin==1&&adordeerid!=0){
        num5+=1
      }
      if(iseffective==1&&isbilling==1&&iswin==1){
        num8+=winprice/1000
      }
      if(iseffective==1&&isbilling==1&&iswin==1){
        num9+=adpayment/1000
      }

    List(num4,num5,num8,num9)
    }

}
