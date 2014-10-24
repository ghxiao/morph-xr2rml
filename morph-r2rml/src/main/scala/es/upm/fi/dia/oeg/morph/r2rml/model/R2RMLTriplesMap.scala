package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import com.hp.hpl.jena.rdf.model.Resource
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import java.sql.DatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseLogicalTable
import es.upm.fi.dia.oeg.morph.base.model.IConceptMapping
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping

class R2RMLTriplesMap(val logicalTable: R2RMLLogicalTable, val subjectMap: R2RMLSubjectMap, val predicateObjectMaps: Set[R2RMLPredicateObjectMap])
        extends MorphBaseClassMapping(predicateObjectMaps) with MorphR2RMLElement with IConceptMapping {

    val logger = Logger.getLogger(this.getClass());

    def buildMetaData(dbMetadata: Option[MorphDatabaseMetaData]) = {
        logger.debug("Building metadata for TriplesMap: " + this.name);

        this.logicalTable.buildMetaData(dbMetadata);
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
        this.logicalTable.getLogicalTableSize();
    }

    override def getMappedClassURIs(): Iterable[String] = {
        this.subjectMap.classURIs;
    }

    override def getTableMetaData(): Option[MorphTableMetaData] = {
        this.logicalTable.tableMetaData;
    }

    def getLogicalTable(): R2RMLLogicalTable = {
        this.logicalTable;
    }

    override def getSubjectReferencedColumns(): List[String] = {
        this.subjectMap.getReferencedColumns();
    }

}

object R2RMLTriplesMap {
    val logger = Logger.getLogger(this.getClass().getName());

    def apply(tmResource: Resource): R2RMLTriplesMap = {

        // LOGICAL TABLE
        val logTabStmt = tmResource.getProperty(Constants.R2RML_LOGICALTABLE_PROPERTY);
        if (logTabStmt == null) {
            val errorMessage = "Missing rr:logicalTable";
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }

        val logTabStmtObject = logTabStmt.getObject();
        val logTabStmtObjectResource = logTabStmtObject.asInstanceOf[Resource];
        val logTab = R2RMLLogicalTable.parse(logTabStmtObjectResource);

        // SUBJECT MAP
        val subjectMaps = R2RMLSubjectMap.extractSubjectMaps(tmResource, logTab.getFormat);
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

        //rr:predicateObjectMap SET
        val predicateObjectMapStatements = tmResource.listProperties(
            Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY);
        val predicateObjectMaps = if (predicateObjectMapStatements != null) {
            predicateObjectMapStatements.toList().map(predicateObjectMapStatement => {
                val predicateObjectMapStatementObjectResource =
                    predicateObjectMapStatement.getObject().asInstanceOf[Resource];
                val predicateObjectMap = R2RMLPredicateObjectMap(predicateObjectMapStatementObjectResource, logTab.getFormat);
                predicateObjectMap;
            });
        } else {
            Set.empty;
        };

        val tm = new R2RMLTriplesMap(logTab, subjectMap, predicateObjectMaps.toSet);
        tm.resource = tmResource
        tm.name = tmResource.getLocalName();
        tm;
    }

}

