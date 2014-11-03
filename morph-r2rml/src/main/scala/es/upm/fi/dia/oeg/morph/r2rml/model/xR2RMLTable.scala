package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants

class xR2RMLTable(
    tableName: String,
    refFormulation: String)
        extends xR2RMLLogicalSource(Constants.LogicalTableType.TABLE_NAME, refFormulation, None) {

    override def getValue(): String = { this.tableName; }
}
