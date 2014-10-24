package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLSQLQuery(sqlQuery: String, format: Option[String])
  extends R2RMLLogicalTable(Constants.LogicalTableType.QUERY_STRING, format: Option[String]) {

  // xR2RML
  // Verify if the format is xrr:Row
  if (!this.getFormat.equals(xR2RML_Constants.xR2RML_RRXROW_URI)) {
    throw new Exception("Illegal format " + this.getFormat + " used with rr:sqlQuery. The format must be " + xR2RML_Constants.xR2RML_RRXROW_URI);
  }
  // end of xR2RML
  override def getValue(): String = { this.sqlQuery; }
}
