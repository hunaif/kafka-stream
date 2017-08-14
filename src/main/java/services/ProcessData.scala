package services

import common.utils.MapperUtil

/**
  * Created by muhammed on 12/8/17.
  */
object ProcessData {

  val refNum = "clientToken"
  val anonymous = "anonymous"
  val createKeyValuePair = (event:String)  => {
    val eventNode = MapperUtil.getJsonNodeFromString(event)
    val referenceNum = if(eventNode.has(refNum)) eventNode.get(refNum).textValue() else anonymous
    (referenceNum,1)
  }

}
