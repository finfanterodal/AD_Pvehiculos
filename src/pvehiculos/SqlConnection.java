/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pvehiculos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author oracle
 */
public class SqlConnection {

    public SqlConnection() {
    }

    static final String driver = "jdbc:oracle:thin:";
    static final String host = "localhost.localdomain"; // tambien puede ser una ip como "192.168.1.14"
    static final String porto = "1521";
    static final String sid = "orcl";
    static final String usuario = "hr";
    static final String password = "hr";
    static final String url = driver + usuario + "/" + password + "@" + host + ":" + porto + ":" + sid;
    //private static final String JDBC_DRIVER 
    private static final String JDBC_URL = url;
   // private static final Driver driver = null;

    //Carga del driver y conexión
    /**
     * Método static que devuelve un objeto de tipo Conection y lanza una
     * excepción por si no es posible establecer la conexión que es recogida por
     * los métodos de la clase LibroDaoJDBC que lo llaman.
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL);
    }

    //Close objeto ResulSet
    /**
     * Método que cierra el objeto de tipo ResultSet.
     *
     * @param rs
     */
    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }
    //Cerramos el objeto PreparedStatement

    /**
     * Método que cierra el objeto de tipo PreparedStatement.
     *
     * @param stmt
     */
    public static void close(PreparedStatement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }
    //Cerramos la conexión

    /**
     * Método que cierra la conexión, el objeto de tipo SqlConnection.
     *
     * @param conn
     */
    public static void close(java.sql.Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

}
