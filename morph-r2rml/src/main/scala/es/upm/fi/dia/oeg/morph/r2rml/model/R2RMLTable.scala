package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLTable(tableName: String)
        extends xR2RMLLogicalSource(Constants.LogicalTableType.TABLE_NAME, xR2RML_Constants.xR2RML_COLUMN_URI, None) {

    if (tableName == null || tableName.equals("")) {
        throw new Exception("Table name not specified");
    }

    override def getValue(): String = { this.tableName; }
}