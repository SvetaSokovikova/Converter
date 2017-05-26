package com.mycompany.neo4j_query;

import java.io.File;

import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.RelationshipType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class Main {
    
    public static void main(String[] args){
        
        String graphDbName = "C:/Films"; // Name of graph database (parameter)
        String tableDbName = "jdbc:postgresql://127.0.0.1:5432/Films"; // Name of retational database (parameter)
        
        String t = "type";
        
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        File location = new File(graphDbName); 
        GraphDatabaseService graphDb = 
                dbFactory.newEmbeddedDatabase(location);
        
        Map<String,Object> row;
        Map<String,Object> cluster;
        Map<String,Object> all_props;
        Iterable<Relationship> rel_iterator;
        Node n;
        String query;
        Result result;
        Result clusters;
        long type;
        
        String url = tableDbName;
        String user = "postgres"; // User name in PostgreSQL
        String password = "Anjelika"; //Password in PostgreSQL
        Connection conn = null;
        Statement stmt = null;
        HashSet cols = new HashSet();
       
        try{
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
        } catch(Exception e){
            e.printStackTrace();
        }
        
        try{
            stmt.execute("create table navigate(unique_identifier bigint primary key, type integer)");
        } 
        catch (Exception e){
            e.printStackTrace();
        }

        // Find relationship of what types can be not-single from one vertex; begin of block
        Result r; 
        Map<String,Object> r_row;
        long starts;
        long edges;
        
        ResourceIterable<RelationshipType> rel_types;
        HashSet<String> moreThanOne = new HashSet();
        
        try(Transaction tx = graphDb.beginTx()){
            rel_types = graphDb.getAllRelationshipTypes();
            
            for (RelationshipType rt: rel_types){
                r = graphDb.execute ("match (a)-[r:"+rt.name()+"]->(b) return count(distinct r), count(distinct a)");
                r_row = r.next();
                edges = (long) r_row.get(r.columns().get(0));
                starts = (long) r_row.get(r.columns().get(1));
                if (starts < edges)
                    moreThanOne.add(rt.name());
            }
        }
        // end of block
        
        clusters = graphDb.execute("match (n) return distinct n."+t+" as one_type");
        
        long progress = 0;
        
        while (clusters.hasNext()){
            cluster = clusters.next();
            type = (long) cluster.get("one_type"); 
            query = "match (n) where n."+t+" = "+type+" return n as the_node";   
        
            result = graphDb.execute(query);
        
            try{
                stmt.execute("create table t_"+type+" (unique_identifier bigint primary key)"); 
            } catch (Exception e){
                e.printStackTrace();
            }
        
            //!!!cols must be empty here!!!
            cols.clear();
        
            while (result.hasNext()){
                row = result.next();
                n = (Node) row.get("the_node");
                    
                try(Transaction trans = graphDb.beginTx()){
                    all_props = n.getAllProperties();
                    rel_iterator = n.getRelationships(Direction.OUTGOING);
                    InsertValue(all_props,rel_iterator,moreThanOne,cols,stmt,type,n.getId());
                    progress++;
                    System.out.println(progress);
                    trans.success();
                }
            }
        }
    }
    
    public static void InsertValue (Map<String,Object> all_props, Iterable<Relationship> rel_iterator,
            HashSet<String> moreThanOne,
            HashSet cols,Statement stmt, long node_type, long identifier){
        
        try{
            stmt.execute("insert into navigate (unique_identifier, type) values (" + identifier + ", "+String.valueOf(node_type)+")");
        } 
        catch (Exception e){
            e.printStackTrace();
        }
        
        String insert_query = "insert into t_"+String.valueOf(node_type)+" (unique_identifier, ";
        String value_fragment = " values ("+identifier+", ";
        String attr_name;
        
        for (String key : all_props.keySet()){
            
            boolean b = false;
            if (key.length()<4)
                b = true;
            else if (!key.substring(0,4).equals("type"))
                b = true;
                
            if (b){
            
                String type = PostgresType(all_props.get(key).getClass());
            
                if (!cols.contains(key)){
                    try{
                        System.out.println(all_props.get(key).getClass()); // Output types
                        stmt.execute("alter table t_"+String.valueOf(node_type)+
                                " add "+key+" "+type+" NULL");
                        cols.add(key);        
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }
                }
                
                insert_query = insert_query + key + ", ";
                if (type == "text")
                    value_fragment = value_fragment + "'" + all_props.get(key).toString().replace("'", "''") + "'" + ", ";
                else
                    value_fragment = value_fragment + all_props.get(key).toString() + ", ";
            } 
        }
        
        Map<String,HashSet<String>> multipled = new HashMap();
        
        for (Relationship r: rel_iterator){            
            
            if (!moreThanOne.contains(r.getType().name())){
             
                attr_name = r.getType().toString()+"_id";
            
                if (!cols.contains(attr_name)){
                    try{
                        stmt.execute("alter table t_"+String.valueOf(node_type)+
                               " add "+attr_name+" bigint NULL"); 
                        cols.add(attr_name);
                    }
                    catch (Exception e){
                       e.printStackTrace();
                    }
                }
                
                insert_query = insert_query + attr_name + ", ";
                value_fragment = value_fragment + r.getEndNode().getId() + ", ";
            }
            else{
                attr_name = r.getType().name()+"_id";
                if (!multipled.containsKey(attr_name)){
                    multipled.put(attr_name, new HashSet());
                    multipled.get(attr_name).add(String.valueOf(r.getEndNode().getId()));
                }
                else
                    multipled.get(attr_name).add(String.valueOf(r.getEndNode().getId()));
            }
        }
        
        for (String str: multipled.keySet()){
            if (!cols.contains(str)){
                try{
                    stmt.execute("alter table t_"+node_type+" add "+str+" jsonb NULL");
                    cols.add(str);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
            insert_query += str+", ";
            String s = "'[";
            for (Iterator iter=multipled.get(str).iterator(); iter.hasNext();){
                s += iter.next()+",";
            }
            s = s.substring(0, s.length()-1);
            s += "]'";
            value_fragment += s+", ";
        }
        
        insert_query = insert_query.substring(0, insert_query.length()-2);
        insert_query = insert_query + ")";
        value_fragment = value_fragment.substring(0, value_fragment.length()-2);
        value_fragment = value_fragment + ")";
        
        insert_query = insert_query + value_fragment;
        
        try{
            stmt.execute(insert_query);
        } 
        catch (Exception e){
            System.out.println(insert_query);
            e.printStackTrace();
        }
    }
    
    public static String PostgresType(Class c){
        String s = "abc";
        java.lang.Integer i = 100;
        java.lang.Long l = i.longValue();
        java.lang.Double d = 1.2222222;
        java.lang.Float f = d.floatValue();
        java.lang.Boolean b = true;
        if (c == s.getClass()) return "text";
        if (c == i.getClass()) return "integer";
        if (c == l.getClass()) return "bigint";
        if (c == d.getClass()) return "FLOAT";
        if (c == f.getClass()) return "REAL";
        if (c == b.getClass()) return "boolean";
        return " ";
    }
    
} 