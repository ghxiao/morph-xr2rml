package fr.unice.i3s.morph.xr2rml.mongo

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
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.GenericQuery

object MongoUtils {

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
        //val dbDriver = configurationProperties.databaseDriver;
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

    /**
     * Execute a MongoDB query against the connection.
     * Returns a list or results as JSON strings
     */
    def execute(connection: GenericConnection, query: GenericQuery): Iterator[String] = {

        val mongoQ = query.concreteQuery.asInstanceOf[MongoDBQuery]
        val jongoCnx = connection.concreteCnx.asInstanceOf[Jongo]

        val collec: MongoCollection = jongoCnx.getCollection(mongoQ.collection)
        val results: MongoCursor[String] = collec.find(mongoQ.query).map[String](jongoHandler)

        results.iterator()
    }
}