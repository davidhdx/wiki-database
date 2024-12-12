from modules.database import URI, AUTH, cleanDatabase
from neo4j import GraphDatabase

with GraphDatabase.driver(URI, auth=AUTH, max_connection_lifetime=30*3600, connection_timeout=60*100) as driver:
    with driver.session() as session:
        print("Database Connected Successfully")
        YesOrNo = input("Do you confirm to delete every node and relation in the database? (Y/n): ")
        
        if YesOrNo == "Y":
            cleanDatabase(session)
        else:
            print("Canceled")


