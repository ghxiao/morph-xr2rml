package es.upm.fi.dia.oeg.morph.r2rml.model
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class xR2RMLJoinParse(parseType: Option[String], joinValue: String, format: Option[String]) {
  val logger = Logger.getLogger(this.getClass().getName());

  var parset = this.getParseType.get
  var form = this.getJoinParseFormat
  if (parset != null) {
    if (!parset.equals(Constants.R2RML_LITERAL_URI) && !parset.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {
      throw new Exception("Illegal parsetype value in joinParse \n " + parset + " is not a correct value");
    }
  }

  if (parset != null && parset.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {
    if (form != null && form.equals(xR2RML_Constants.xR2RML_RRXROW_URI)) {
      throw new Exception("Illegal format value in joinParse \n " + form + " is not a correct value for a parseType " + parset);
    }
  }

  def getParseType() = {
    logger.info(" method recursive getParseType ");
    if (this.parseType != null) {
      this.parseType
    } else {
      Some(Constants.R2RML_LITERAL_URI)
    }
  }

  def getJoinValue(): String = {
    joinValue
  }

  def getJoinParseFormat(): String = {

    if (format != null && format.isDefined) {
      format.get
    } else {
      xR2RML_Constants.xR2RML_RRXROW_URI
    }
  }
}

