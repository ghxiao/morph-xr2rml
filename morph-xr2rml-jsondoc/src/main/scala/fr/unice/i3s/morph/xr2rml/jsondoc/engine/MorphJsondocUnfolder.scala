package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLLogicalSource
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLTable
import fr.unice.i3s.morph.xr2rml.jsondoc.mongo.MongoUtils

class MorphJsondocUnfolder(md: R2RMLMappingDocument, properties: MorphProperties)
        extends MorphBaseUnfolder(md, properties) {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Parse the query string provided in the mapping and build an instance of GenericQuery containing
     * a MongoDBQuery for the case of MongoDB, to be extended for other types of db.
     * @return GenericQuery instance corresponding to the query provided in the logical source
     */
    @throws[MorphException]
    override def unfoldConceptMapping(cm: R2RMLTriplesMap): GenericQuery = {

        val triplesMap = cm.asInstanceOf[R2RMLTriplesMap]
        if (logger.isDebugEnabled()) logger.debug("Unfolding triples map " + triplesMap.toString)
        val logicalSrc = triplesMap.logicalSource.asInstanceOf[xR2RMLLogicalSource];

        val logicalSrcQuery: String = logicalSrc match {
            case _: xR2RMLTable => {
                logger.error("Logical source with table name not allowed in the context of a JSON document database: a query is expected")
                null
            }
            case _: xR2RMLQuery => {
                val encldChar = Constants.getEnclosedCharacter(dbType);
                val query = logicalSrc.getValue().replaceAll("\"", encldChar);
                query
            }
            case _ => { throw new MorphException("Unknown logical table/source type: " + logicalSrc) }
        }

        val query = dbType match {
            case Constants.DATABASE_MONGODB => {
                val mongoQuery = MongoUtils.parseQueryString(logicalSrcQuery)
                logger.info("Query for triples map " + cm.id + ": " + mongoQuery.toString)
                new GenericQuery(Constants.DatabaseType.MongoDB, mongoQuery)
            }
            case _ =>
                throw new MorphException("Database type not supported: " + dbType)
        }
        query
    }
}