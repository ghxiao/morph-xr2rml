package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.setAsJavaSet

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.Resource

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.IConceptMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor

class R2RMLTriplesMap(
    val logicalSource: xR2RMLLogicalSource,
    val subjectMap: R2RMLSubjectMap,
    val predicateObjectMaps: Set[R2RMLPredicateObjectMap])

        extends MorphBaseClassMapping(predicateObjectMaps) with MorphR2RMLElement with IConceptMapping {

    val logger = Logger.getLogger(this.getClass());

    def buildMetaData(dbMetadata: Option[MorphDatabaseMetaData]) = {
        logger.debug("Building metadata for TriplesMap: " + this.name);
        this.logicalSource.buildMetaData(dbMetadata);
    }

    def accept(visitor: MorphR2RMLElementVisitor): Object = { return visitor.visit(this); }

    override def toString(): String = { return this.name }

    override def getConceptName(): String = {
        var result: String = null;

        val classURIs = this.subjectMap.classURIs;
        if (classURIs == null || classURIs.size() == 0) {
            logger.warn("No class URI defined for TriplesMap: " + this);
        } else {
            if (classURIs.size() > 1) {
                logger.warn("Multiple classURIs defined, only one is returned!");
            }
            result = classURIs.iterator().next();
        }

        return result;
    }

    override def getPropertyMappings(propertyURI: String): Iterable[MorphBasePropertyMapping] = {
        val poms = this.predicateObjectMaps;
        val result = poms.filter(pom => {
            val predicateMapValue = pom.getPredicateMap(0).getOriginalValue();
            predicateMapValue.equals(propertyURI)
        })

        result;
    }

    override def getPropertyMappings(): Iterable[MorphBasePropertyMapping] = {
        this.predicateObjectMaps
    }

    override def isPossibleInstance(uri: String): Boolean = {
        var result = false;

        val subjectMapTermMapType = this.subjectMap.termMapType;
        if (subjectMapTermMapType == Constants.MorphTermMapType.TemplateTermMap) {
            val templateValues = this.subjectMap.getTemplateValues(uri);
            if (templateValues != null && templateValues.size() > 0) {
                result = true;
                for (value <- templateValues.values()) {
                    if (value.contains("/")) {
                        result = false;
                    }
                }
            }
        } else {
            result = false;
            val errorMessage = "Can't determine whether " + uri + " is a possible instance of " + this.toString();
            logger.warn(errorMessage);
        }

        result;
    }

    override def getLogicalTableSize(): Long = {
        this.logicalSource.getLogicalTableSize();
    }

    override def getMappedClassURIs(): Iterable[String] = {
        this.subjectMap.classURIs;
    }

    override def getTableMetaData(): Option[MorphTableMetaData] = {
        this.logicalSource.tableMetaData;
    }

    def getLogicalSource(): xR2RMLLogicalSource = {
        this.logicalSource;
    }

    override def getSubjectReferencedColumns(): List[String] = {
        this.subjectMap.getReferencedColumns();
    }
}

object R2RMLTriplesMap {
    val logger = Logger.getLogger(this.getClass().getName());

    def apply(tmResource: Resource): R2RMLTriplesMap = {
        logger.debug("Parsing triples map: " + tmResource.getLocalName())

        // --- Look for Logical table (rr:logicalTable) or Logical source (xrr:logicalSource) definition
        val logTabStmt = tmResource.getProperty(Constants.R2RML_LOGICALTABLE_PROPERTY);
        val logSrcStmt = tmResource.getProperty(xR2RML_Constants.xR2RML_LOGICALSOURCE_PROPERTY);
        var logSource: xR2RMLLogicalSource = {
            if (logTabStmt != null) {
                val res = logTabStmt.getObject().asInstanceOf[Resource]
                xR2RMLLogicalSource.parse(res, Constants.R2RML_LOGICALTABLE_URI)
            } else if (logSrcStmt != null) {
                val res = logSrcStmt.getObject().asInstanceOf[Resource]
                xR2RMLLogicalSource.parse(res, xR2RML_Constants.xR2RML_LOGICALSOURCE_URI)
            } else {
                val errorMessage = "Missing rr:logicalTable and xrr:logicalSource"
                logger.error(errorMessage)
                throw new Exception(errorMessage)
            }
        }
        logger.trace("Parsed logical table/source. " + logSource.toString)

        // --- Subject map
        val subjectMaps = R2RMLSubjectMap.extractSubjectMaps(tmResource, logSource.refFormulation)
        if (subjectMaps == null) {
            val errorMessage = "Missing rr:subjectMap";
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }
        if (subjectMaps.size > 1) {
            val errorMessage = "Multiple rr:subjectMap predicates are not allowed";
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }
        val subjectMap = subjectMaps.iterator.next;

        // --- Predicate-Object Maps
        logger.trace("Parsing predicate-object maps")
        val pomStmts = tmResource.listProperties(Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY);
        val predicateObjectMaps = if (pomStmts != null) {
            pomStmts.toList().map(pomStmt => {
                val pomObjectRes = pomStmt.getObject().asInstanceOf[Resource]
                val predicateObjectMap = R2RMLPredicateObjectMap(pomObjectRes, logSource.refFormulation)
                predicateObjectMap
            });
        } else {
            Set.empty
        };

        val tm = new R2RMLTriplesMap(logSource, subjectMap, predicateObjectMaps.toSet);
        tm.resource = tmResource
        tm.name = tmResource.getLocalName();
        logger.debug("Completed parsing triples map: " + tmResource.getLocalName())
        tm;
    }
}

