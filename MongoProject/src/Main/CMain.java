package Main;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

public class CMain {

	public static void main(String[] args) {

		MongoClient mongoClient;
		
		try {
			
			mongoClient = new MongoClient( "localhost", 27017 );
			DB db = mongoClient.getDB("testdb");
			
			//List databases
			List<String> dbs = mongoClient.getDatabaseNames();
			
			for(String strDB : dbs){
			
				System.out.println( strDB );
			
			}			
			
			//List tables (Collection)
			Set<String> tables = db.getCollectionNames();
			 
			for(String strCol : tables){
				
				System.out.println( strCol );
				
			}			
			
			CData Data = new CData();
			
			Data.strFirstName = "Tomás";
			Data.strLastName = "Moreno";
			
			boolean toJson = true;
			
			XStream xstream = null;
			
			if ( toJson == false ) {
			
			   xstream = new XStream();
			
			}
			else {
				
			   xstream = new XStream( new JettisonMappedXmlDriver() );
			   
			   xstream.setMode( XStream.NO_REFERENCES );
				
			}
			
			xstream.alias( "CData", CData.class );
			
			String strXMLObject = xstream.toXML( Data );

			Data.strFirstName = "Loly";
			Data.strLastName = "Gómez";
			
			final String strKeyData = UUID.randomUUID().toString();
			
			long lngCreatedTime = new Date().getTime()/1000;
			long lngTTL = lngCreatedTime + 4;
			
			if ( db.collectionExists( "ObjectCache" ) )
				db.getCollection( "ObjectCache" ).drop();
			
			final DBCollection table = db.getCollection( "ObjectCache" );
			
			table.drop();
			
			BasicDBObject document = new BasicDBObject();
			document.put( "KeyData", strKeyData );
			document.put( "Data", strXMLObject );
			document.put( "createdSecondsTime", lngCreatedTime );
			document.put( "timeToLive", lngTTL );
			table.insert( document );
			
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put( "KeyData", strKeyData );
			
			DBCursor cursor = table.find( searchQuery );
			
			CData Data1 = null;
			
			while ( cursor.hasNext() ) {
			
				DBObject DBObject = cursor.next();
				
				//System.out.println( cursor.next() );
				strXMLObject = (String) DBObject.get( "Data" );
				
				System.out.println( strXMLObject );
			
				Data1 = (CData) xstream.fromXML( strXMLObject );
				
			}			
			
			System.out.println( Data1.strFirstName );
			System.out.println( Data1.strLastName );
			
			//Test autoremove
			
			int intCount = 0;
			
			Timer timer = new Timer();
			
			timer.schedule( new TimerTask() {
				  
				  boolean bLock = false;
				
				  @Override
				  public void run() {

					  if ( bLock == false ) {

						  bLock = true;
						  
						  long lngEnlapsedSeconds = new Date().getTime()/1000;

						  DBCursor cursor = table.find();

						  while ( cursor.hasNext() ) {

							  DBObject DBObject = cursor.next();

							  long lngTTL = (long) DBObject.get( "timeToLive" );

							  if ( lngTTL < lngEnlapsedSeconds ) {

								  table.remove( DBObject );

							  }

						  }
					  
						  bLock = false;
						  
					  }
					
				  }
				
				}, 1000, 2000 );
			
			while ( true ) {
				
				if ( intCount < 1000 ) {
					
					cursor = table.find();
					
					while ( cursor.hasNext() ) {
					
						DBObject DBObject = cursor.next();

						//System.out.println( cursor.next() );
						strXMLObject = (String) DBObject.get( "Data" );

						System.out.println( strXMLObject );

						//Data1 = (CData) xstream.fromXML( strXMLObject );
						
					}			
					
				}
				else {
					
					break;
					
				} 
				
				intCount += 1;
				
				Thread.sleep( 1000 );
				
			}
			
			/*boolean bAuth = db.authenticate("username", "password".toCharArray());
			
			if ( bAuth ) {
				
				System.out.println( "Conectado..." );
				
			}*/
			
		}
		catch ( Exception Ex) {

			Ex.printStackTrace();
			
		}
		
	}

}
