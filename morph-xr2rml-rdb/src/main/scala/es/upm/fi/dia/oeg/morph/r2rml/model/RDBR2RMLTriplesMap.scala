package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.setAsJavaSet

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.Resource

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData

class RDBR2RMLTriplesMap(
    resource: Resource,
    logicalSource: RDBxR2RMLLogicalSource,
    refFormulation: String,
    subjectMap: R2RMLSubjectMap,
    predicateObjectMaps: Set[R2RMLPredicateObjectMap])

        extends R2RMLTriplesMap(resource, logicalSource, refFormulation, subjectMap, predicateObjectMaps) {

    def getLogicalTableSize(): Long = {
        this.logicalSource.getLogicalTableSize
    }

    def getTableMetaData(): Option[MorphTableMetaData] = {
        this.logicalSource.tableMetaData;
    }

    override def getLogicalSource(): RDBxR2RMLLogicalSource = {
        this.logicalSource;
    }
}

