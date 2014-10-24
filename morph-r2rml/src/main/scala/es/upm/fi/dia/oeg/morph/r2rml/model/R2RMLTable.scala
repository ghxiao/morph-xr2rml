package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLTable(tableName: String, format: Option[String])
  extends R2RMLLogicalTable(Constants.LogicalTableType.TABLE_NAME, format: Option[String]) {
  if (tableName == null || tableName.equals("")) {
    throw new Exception("Empty table name specified!");
  }

  /**  @note xR2RML */
  // Verify if the format is xrr:Row
  if (!this.getFormat.equals(xR2RML_Constants.xR2RML_RRXROW_URI)) {
    throw new Exception("Illegal format " + this.getFormat + " used with rr:tableName. The format must be " + xR2RML_Constants.xR2RML_RRXROW_URI);
  }
  // end xR2RML

  override def getValue(): String = { this.tableName; }
}