package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import java.util.Collection
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLTable
import es.upm.fi.dia.oeg.morph.base.GenericQuery

class MorphJsondocUnfolder(md: R2RMLMappingDocument, properties: MorphProperties)
        extends MorphBaseUnfolder(md, properties) with MorphR2RMLElementVisitor {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Unfolding a triples map in the case of a NoSQL document database simply means to retrieve the query string
     * @return query string provided in the logical source with possible replacements of enclosing characters
     */
    private def unfoldTriplesMap(
        triplesMapId: String,
        logicalSrc: xR2RMLLogicalSource,
        subjectMap: R2RMLSubjectMap,
        poms: Collection[R2RMLPredicateObjectMap]): String = {

        val logicalSrcUnfolded: String = logicalSrc match {
            case _: xR2RMLTable => {
                logger.error("Table name not allowed in the context of a JSON document database: a query is expected")
                null
            }
            case _: xR2RMLQuery => {
                val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
                val query = logicalSrc.getValue().replaceAll("\"", dbEnclosedCharacter);
                query
            }
            case _ => { throw new Exception("Unknown logical table/source type: " + logicalSrc) }
        }
        logicalSrcUnfolded
    }

    /**
     * Entry point of the unfolder in the data materialization case
     */
    override def unfoldConceptMapping(cm: MorphBaseClassMapping): GenericQuery = {

        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap]
        logger.debug("Unfolding triples map " + triplesMap.toString)
        val logicalTable = triplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];
        val resultAux = this.unfoldTriplesMap(triplesMap.id, logicalTable, triplesMap.subjectMap, triplesMap.predicateObjectMaps);

        logger.info("Query for triples map " + cm.id + ": " + resultAux.replaceAll("\n", " "))
        new GenericQuery(Constants.DatabaseType.MongoDB, resultAux)
    }

    def visit(logicalTable: xR2RMLLogicalSource): SQLLogicalTable = {
        throw new Exception("Unsopported method.")
    }

    def visit(md: R2RMLMappingDocument): Collection[IQuery] = {
        throw new Exception("Unsopported method.")
    }

    def visit(objectMap: R2RMLObjectMap): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(refObjectMap: R2RMLRefObjectMap): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(r2rmlTermMap: R2RMLTermMap): Object = {
        throw new Exception("Unsopported method.")
    }

    def visit(triplesMap: R2RMLTriplesMap): IQuery = {
        throw new Exception("Unsopported method.")
    }
}