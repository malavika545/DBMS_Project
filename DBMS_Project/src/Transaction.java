import java.sql.*;
import java.io.*;
import java.util.*;
import java.lang.*;
/*
 * Team Members: 
 * 		Malavika Rajanala
 * 		Harshitha Rangaraju
 * 
 * Tasks Performed:
 * 1. The product p1 is deleted from Product and Stock.
 * 2. The depot d1 is deleted from Depot and Stock.
 * 5. We add a product (p100, cd, 5) in Product and (p100, d2, 50) in Stock.
 * 6. We add a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock.
 *************************************************************************/

public class Transaction {
    public static void main(String args[]) throws SQLException, IOException,ClassNotFoundException,NullPointerException  {
    	
        //initializing selected option
        int selected_option = 0;
        // Load the PostgreSQL driver
        Class.forName("org.postgresql.Driver");

        // Connect to the default database with credentials
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/", "postgres", "Qwerty");

        // For atomicity
        conn.setAutoCommit(false);

        // For isolation
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Statement stmt_obj = null;

        //DROP TABLE IF EXISTS
        String dropTableIfExist = "DROP TABLE IF EXISTS Product, Depot, Stock;";

        //initializing DB with Tables, Data and setting constraints
        try {
            // Create statement object
            stmt_obj = conn.createStatement();
            stmt_obj.executeUpdate(dropTableIfExist);
        } catch (SQLException e) {
            System.out.println("\nTables exists!");
        }
        conn.commit();//committing changes if the table is created
        stmt_obj.close();
        boolean x = true;
        //Menu Driven Loop
        while (x) {
            stmt_obj = null;// Resetting statement object to null
            //Scanner for menu input
            Scanner input_option = new Scanner(System.in);
            System.out.println("Select from the following Options:");
            System.out.println("1. Create tables for product, depot, stock\n" +
            		"2. Alter table by adding primary keys and foreign keys\n" +
            		"3. Insert the data into the table(product,depot,stock)\n" +
            		"4. The product p1 is deleted from Product and Stock\n" +
                    "5. The depot 'd1' is deleted from 'Depot' and 'Stock'\n" +
            		"6. Add a product (p100, cd, 5) in Product and (p100, d2, 50) in Stock\n" +
                    "7. Add a 'depot' (d100, Chicago, 100) in 'Depot' and (p1, d100, 100) in 'Stock'\n" +
                    "8. Retrive all the tables\n" +
                    "9. Exit\n");
            System.out.println("Enter Option number to perform tasks : ");
            selected_option = input_option.nextInt();
            if (selected_option < 9) {
                System.out.println("----------------------------------------------------------------------");
                System.out.println("Performing Query...");
            }
            //Switch case on user input
            switch (selected_option) {
            case 1:
                System.out.println("...Creating Tables PRODUCT, DEPOT, STOCK...");
                try {
                    stmt_obj = conn.createStatement();
                    String createTableIfNot = "CREATE TABLE IF NOT EXISTS Product(prodid CHAR(10), pname VARCHAR(30), price DECIMAL);" +
                            "CREATE TABLE IF NOT EXISTS Depot(depid CHAR(10), addr VARCHAR(40), volume INTEGER);" +
                            "CREATE TABLE IF NOT EXISTS Stock(prodid CHAR(10), depid CHAR(10), quantity INTEGER);";
                    stmt_obj.executeUpdate(createTableIfNot);
                    conn.commit();
                    stmt_obj.close();
                } catch (SQLException e) {
                    System.out.println("An exception was thrown" + e.getMessage());
                    // For atomicity
                    conn.rollback();
                    stmt_obj.close();
                }
                System.out.println("Tables are created!!");
                System.out.println("----------------------------------------------------------------------");
                break;
            case 2:
                System.out.println("...Altering tables...");
                try {
                    stmt_obj = conn.createStatement();
                    String alterCasc = "ALTER TABLE Product ADD CONSTRAINT pk_product PRIMARY KEY(prodid);" +
                            "ALTER TABLE Depot ADD CONSTRAINT pk_depot PRIMARY KEY(depid);" +
                            "ALTER TABLE Stock ADD CONSTRAINT pk_stock PRIMARY KEY(prodid,depid);" +
                            "ALTER TABLE Stock ADD CONSTRAINT fk_stock_product_id FOREIGN KEY(prodid) REFERENCES Product(prodid)ON DELETE CASCADE ON UPDATE CASCADE;" +
                            "ALTER TABLE Stock ADD CONSTRAINT fk_stock_depot_id FOREIGN KEY(depid) REFERENCES Depot(depid)ON DELETE CASCADE ON UPDATE CASCADE;;";
                    
                    stmt_obj.executeUpdate(alterCasc);
                    conn.commit();
                    stmt_obj.close();
                } catch (SQLException e) {
                    System.out.println("An exception was thrown" + e.getMessage());
                    // For atomicity
                    conn.rollback();
                    stmt_obj.close();
                }
                System.out.println("Altering is done!!");
                System.out.println("----------------------------------------------------------------------");
                break;
            case 3:
                System.out.println("...Inserting values into the tables...");
                try {
                    stmt_obj = conn.createStatement();
                    String insertTableData = "INSERT INTO Product(prodid, pname, price) VALUES" +
                            "('p1','tape',2.5)," +
                            "('p2','tv',250)," +
                            "('p3','vcr',80);" +
                            "INSERT INTO Depot(depid, addr, volume) VALUES" +
                            "('d1','New York',9000)," +
                            "('d2','Syracuse',6000)," +
                            "('d4','New York',2000);" +
                            "INSERT INTO Stock(prodid, depid, quantity) VALUES" +
                            "('p1','d1',1000)," +
                            "('p1','d2',-100)," +
                            "('p1','d4',1200)," +
                            "('p3','d1',3000)," +
                            "('p3','d4',2000)," +
                            "('p2','d4',1500)," +
                            "('p2','d1',-400)," +
                            "('p2','d2',2000);";
                    stmt_obj.executeUpdate(insertTableData);
                    conn.commit();
                    stmt_obj.close();
                } catch (SQLException e) {
                    System.out.println("An exception was thrown" + e.getMessage());
                    // For atomicity
                    conn.rollback();
                    stmt_obj.close();
                }
                System.out.println("Inserted data into the tables!!");
                System.out.println("----------------------------------------------------------------------");
                break;
            	
                case 4:
                    System.out.println("Deleting the product 'p1' from product and stock tables.....");
                    try {
                        stmt_obj = conn.createStatement();
                        stmt_obj.executeUpdate("DELETE FROM product WHERE prodid ='p1';");
                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        System.out.println("Rolling back..............");
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }
                    System.out.println("----------------------------------------------------------------------");
                    break;
                case 5:
                    //Delete case Task 4
                    System.out.println("Deleting the depot 'd1' from Depot and Stock........");
                    try {
                        stmt_obj = conn.createStatement();
                        stmt_obj.executeUpdate("DELETE FROM depot WHERE depid ='d1';");
                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        System.out.println("Rolling back..............!");
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }
                    System.out.println("----------------------------------------------------------------------");
                    break;

                case 6:
                    System.out.println("Adding a product (p100, cd, 5) in product and (p100, d2, 50) in Stock");
                    try {
                        stmt_obj = conn.createStatement();
                        stmt_obj.executeUpdate("INSERT INTO product (prodid, pname, price) VALUES ('p100', 'cd', 5);");
                        stmt_obj.executeUpdate("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p100', 'd2', 50);");
                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        System.out.println("Rolling back...........");
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }
                    System.out.println("----------------------------------------------------------------------");
                    break;
                case 7:
                    System.out.println("Adding a depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock.");
                    try {
                        stmt_obj = conn.createStatement();
                        stmt_obj.executeUpdate("INSERT INTO Depot (depid, addr, volume) VALUES ('d100', 'Chicago', 100);");
                        stmt_obj.executeUpdate("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd100', 100);");
                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        System.out.println("Rolling back..............");
                        // For atomicity
                        conn.rollback();
                        System.out.println("In order to add p1 in stock table, we need to add other row in product table as there is a foreign key constraint");
                        System.out.println("After insertion...");
                        stmt_obj = conn.createStatement();
                        stmt_obj.executeUpdate("INSERT INTO Product(prodid, pname, price) VALUES ('p1', 'disk', 10);");
                        stmt_obj.executeUpdate("INSERT INTO Depot (depid, addr, volume) VALUES ('d100', 'Chicago', 100);");
                        stmt_obj.executeUpdate("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd100', 100);");
                        conn.commit();
                        stmt_obj.close();
                    }
                    System.out.println("----------------------------------------------------------------------");                
                    break;
                case 8:
                    //Retrieving table data from postgres
                    System.out.println("Retrieve all the tables from Database");
                    try {
                        stmt_obj = conn.createStatement();
                        ResultSet rs_product = stmt_obj.executeQuery("SELECT * FROM Product;");
                        System.out.println("-------------------------------------");
                        System.out.println("Table Product");
                        while (rs_product.next()) {
                            String product_id = rs_product.getString("prodid").trim();
                            String product_name = rs_product.getString("pname".trim());
                            float product_price = rs_product.getInt("price");
                            System.out.println("prodid: " + product_id + ", pname: " + product_name + ", price: " + product_price);
                        }

                        rs_product.close();

                        ResultSet rs_depot = stmt_obj.executeQuery("SELECT * FROM Depot;");
                        System.out.println("-----------------------------------------");
                        System.out.println("Table Depot");

                        while (rs_depot.next()) {
                            String depot_id = rs_depot.getString("depid").trim();
                            String depot_addr = rs_depot.getString("addr").trim();
                            int depot_volume = rs_depot.getInt("volume");
                            System.out.println("depid: " + depot_id + ", addr: " + depot_addr + ", volume: " + depot_volume);
                        }

                        rs_depot.close();

                        ResultSet rs_stock = stmt_obj.executeQuery("SELECT * FROM Stock;");
                        System.out.println("------------------------------------------");
                        System.out.println("Table Stock");

                        while (rs_stock.next()) {
                            String stock_product_id = rs_stock.getString("prodid").trim();
                            String stock_depid_id = rs_stock.getString("depid").trim();
                            int stock_quantity = rs_stock.getInt("quantity");
                            System.out.println("prodid: " + stock_product_id + ", depid: " + stock_depid_id + ", quantity: " + stock_quantity);
                        }

                        rs_stock.close();

                        conn.commit();
                        stmt_obj.close();
                    } catch (SQLException e) {
                        System.out.println("An exception was thrown" + e.getMessage());
                        // For atomicity
                        conn.rollback();
                        stmt_obj.close();
                    }
                    System.out.println("----------------------------------------------------------------------");
                    break;
                case 9:
                    //Exit
                    System.out.println("Program Terminated!!!!!");
                    x=false;
                    conn.close();
                    break;
                default:
                    System.out.println("Invalid choice");
                    stmt_obj.close();
                    conn.close();
            }
        }
    }
}