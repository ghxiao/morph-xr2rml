package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLSQLQuery(sqlQuery: String)
        extends xR2RMLLogicalSource(Constants.LogicalTableType.SQL_QUERY, xR2RML_Constants.xR2RML_COLUMN_URI, None) {

    override def getValue(): String = { this.sqlQuery; }
}
