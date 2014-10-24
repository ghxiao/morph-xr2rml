package es.upm.fi.dia.oeg.morph.base

import org.apache.log4j.Logger
import org.json.JSONObject
import org.json.JSONException
import org.json.XML

object xR2RML_XML_to_JSON {
  val logger = Logger.getLogger(this.getClass().getName());

  def convertXMLinJSON(data: String): String = {

    var jsondata = "";

    try {
      var xmlJSONObj: JSONObject = XML.toJSONObject(data);
      jsondata = xmlJSONObj.toString(4);
    } catch {
      case e: JSONException => { logger.info(" error JSON format in database or parsing error" + e.toString()); null }
    }

    jsondata

  }
}