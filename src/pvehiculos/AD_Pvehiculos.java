package pvehiculos;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.geojson.Point;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.bson.Document;

/**
 *
 * @author oracle
 */
public class AD_Pvehiculos {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws SQLException {
        //
        double id = 0d;
        String dni = "";
        String codveh = "";
        int pf = 0;
        Clientes cliente = new Clientes();
        Vehiculos vehiculo = new Vehiculos();

        //Conectarse a MONGODB
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection collection = database.getCollection("vendas");

        MongoCursor<Document> cursor = (MongoCursor) collection.find().iterator();
        while (cursor.hasNext()) {
            System.out.println("OBJECT");
            Document doc = cursor.next();
            System.out.println("id: " + doc.getDouble("_id") + " dni: " + doc.getString("dni") + " codveh: " + doc.getString("codveh"));
            id = doc.getDouble("_id");
            dni = doc.getString("dni");
            codveh = doc.getString("codveh");

            //CONSULTAS OBJECT DB CLIENTES
            // Open a database connection
            // (create a new database if it doesn't exist yet):
            EntityManagerFactory emf = Persistence.createEntityManagerFactory("objectdb/db/vehicli.odb");
            EntityManager em = emf.createEntityManager();
            TypedQuery<Clientes> query = em.createQuery("SELECT p FROM Clientes p WHERE p.dni='" + dni + "'", Clientes.class);
            List<Clientes> results = query.getResultList();
            for (Clientes p : results) {
                //Recogemos y guardamos datos de clientes
                cliente.setDni(p.dni);
                cliente.setNcompras(p.ncompras);
                cliente.setNomec(p.nomec);
                System.out.println(cliente.toString());
            }

            //CONSULTAS OBJECT DB VEHICULOS
            TypedQuery<Vehiculos> query2 = em.createQuery("SELECT p FROM Vehiculos p where p.codveh='" + codveh + "'", Vehiculos.class);
            List<Vehiculos> results2 = query2.getResultList();
            for (Vehiculos p : results2) {
                vehiculo.setAnomatricula(p.anomatricula);
                vehiculo.setCodveh(p.codveh);
                vehiculo.setNomveh(p.nomveh);
                vehiculo.setPrezoorixe(p.prezoorixe);
                //Recogemos y guardamos datos de vehiculos
                if (cliente.getNcompras() == 0) {
                    pf = vehiculo.getPrezoorixe() - ((2019 - vehiculo.getAnomatricula()) * 500) - 0;
                } else {
                    pf = vehiculo.getPrezoorixe() - ((2019 - vehiculo.getAnomatricula()) * 500) - 500;
                }
                System.out.println(vehiculo.toString() + " pf: " + pf);
            }
            //INSERT EN LA TABLA DE ORACLE finalveh
            em.close();

            Connection conn = null;
            PreparedStatement stmt = null;
            conn = SqlConnection.getConnection();
            String sql_INSERT = "INSERT INTO finalveh VALUES(?,?,?,tipo_vehf(?,?))";
            stmt = conn.prepareStatement(sql_INSERT);
            stmt.setInt(1, (int) id);
            stmt.setString(2, cliente.getDni());
            stmt.setString(3, cliente.getNomec());
            stmt.setString(4, vehiculo.getNomveh());
            stmt.setInt(5, pf);
            stmt.executeUpdate();
            SqlConnection.close(stmt);
            SqlConnection.close(conn);

        }

        //
    }

}
