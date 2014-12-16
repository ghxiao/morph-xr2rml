package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants

class xR2RMLTable(
    tableName: String)
        extends xR2RMLLogicalSource(Constants.LogicalTableType.TABLE_NAME, Constants.xR2RML_REFFORMULATION_COLUMN, None) {

    override def getValue(): String = { this.tableName; }
}
