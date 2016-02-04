package fr.unice.i3s.morph.xr2rml.mongo.engine

import java.net.URI

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.seqAsJavaList

import org.apache.log4j.Logger
import org.jongo.Jongo
import org.jongo.MongoCollection
import org.jongo.MongoCursor

import com.mongodb.DB
import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import fr.unice.i3s.morph.xr2rml.mongo.JongoResultHandler
import fr.unice.i3s.morph.xr2rml.mongo.MongoDBQuery

class MorphMongoDataSourceReader extends MorphBaseDataSourceReader {

    /**
     * Execute a MongoDB query against the connection.
     * @return iterator on strings representing the result JSON documents.
     * May return an empty result set but not null.
     */
    override def execute(query: GenericQuery): MorphBaseResultSet = {
        val mongoQ = query.concreteQuery.asInstanceOf[MongoDBQuery]
        val jongoCnx = connection.concreteCnx.asInstanceOf[Jongo]

        val collec: MongoCollection = jongoCnx.getCollection(mongoQ.collection)

        val queryStr =
            if (mongoQ.query.startsWith("{") && mongoQ.query.endsWith("}"))
                mongoQ.query
            else
                "{" + mongoQ.query + "}"

        val results: MongoCursor[String] = collec.find(queryStr).map[String](MorphMongoDataSourceReader.jongoHandler)
        new MorphMongoResultSet(results.iterator())
    }

    override def setConnection(connection: GenericConnection) {
        if (!connection.isMongoDB)
            throw new MorphException("Connection type is not MongoDB")
        this.connection = connection
    }

    override def setTimeout(timeout: Int) {
    }

    override def closeConnection() {
    }
}

object MorphMongoDataSourceReader {

    private val logger = Logger.getLogger(this.getClass().getName());

    private val jongoHandler: JongoResultHandler = new JongoResultHandler

    /**
     * Create a Jongo context from a MongoDB connection
     * @return an instance of GenericConnection with dbtype = Constants.DatabaseType.MongoDB
     */
    def createConnection(props: MorphProperties): GenericConnection = {
        val userName = props.databaseUser;
        val dbName = props.databaseName;
        val userPwd = props.databasePassword;
        val dbUrl = props.databaseURL;

        try {
            val uri = new URI(dbUrl);
            val servAdr = new ServerAddress(uri.getHost(), uri.getPort())

            val dbCnx: DB =
                if (userName.isEmpty() && userPwd.isEmpty()) {
                    new MongoClient(servAdr).getDB(dbName)
                } else {
                    val cred = MongoCredential.createMongoCRCredential(userName, dbName, userPwd.toCharArray())
                    new MongoClient(servAdr, seqAsJavaList(List(cred))).getDB(dbName)
                }

            // Create a generic connection with a Jongo context
            val jongoCtx = new Jongo(dbCnx)
            new GenericConnection(Constants.DatabaseType.MongoDB, jongoCtx);

        } catch {
            case e: java.net.URISyntaxException =>
                throw new Exception("Invalid database URL: " + dbUrl + ". Must be formatted as: mongodb://127.0.0.1:27017", e)
            case e: java.net.UnknownHostException =>
                throw new Exception("Cannot connect to the database: " + e.getMessage(), e)
            case e: com.mongodb.MongoException =>
                throw new Exception("Error when connecting to the database: " + e.getMessage(), e)
        }
    }
}