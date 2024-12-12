from modules.utils import parseTime
import time

URI = "bolt://172.23.3.247:7687"
AUTH = ("neo4j", "12345678")
_BATCH_SIZE = int(1e6)

def uploadNodes(session, nodes, batch_size=_BATCH_SIZE):
    '''
    Uploads a list of nodes to the Neo4j database in batches.

    Args:
        session (neo4j.Session): The Neo4j session object.
        nodes (list): A list of dictionaries containing the node data.
        batch_size (int, optional): The number of nodes to upload in each batch. Defaults to _BATCH_SIZE.

    Description:
        This function uploads a list of nodes to the Neo4j database in batches to improve performance.
        It creates a new node for each item in the list and indexes the nodes by their ID.
    '''

    def insertNodes(tx, nodes):
        query = """
        UNWIND $batch AS row
        CREATE (w:WORD {id: row.id, word: row.word})
        """
        tx.run(query, batch=nodes)

    def process_batch(batch):
        session.execute_write(insertNodes, batch)

    print(f"Inserting {len(nodes)/1e6:0.0f}e6 nodes ", end="")
    start = time.time()

    n = len(nodes)
    batches = [nodes[i:min(i + batch_size, n)] for i in range(0, n, batch_size)]
    
    for batch in batches:
        print(".", end="")
        process_batch(batch)

    print(" Finished.")

    print("Indexing nodes... ", end="")

    # Indexing for speed up MATCH queries
    session.run("""
    CREATE INDEX FOR (w:WORD) ON (w.id)
    """)

    print("Finished.")

    # Print elpased time
    fElapsed = parseTime(time.time() - start)
    print(f"Time to insert nodes: [{fElapsed}]")

def uploadRelations(edges_file, session, edge_name, batch_size=_BATCH_SIZE, start_from=0, alert=int(1e6)):
    '''
    Uploads a list of edges from a file to the Neo4j database in batches.

    Args:
        edges_file (str): The path to the file containing the edge data.
        session (neo4j.Session): The Neo4j session object.
        edge_name (str): The name of the edge type to create.
        batch_size (int, optional): The number of edges to upload in each batch. Defaults to _BATCH_SIZE.
        start_from (int, optional): The line number to start reading from in the file. Defaults to 0.
        alert (int, optional): The number of lines to process before printing a progress update. Defaults to 1 million.

    Description:
        This function uploads a list of edges from a file to the Neo4j database in batches to improve performance.
        It reads the edge data from the file, processes it in batches, and creates new edges in the database.
        The function also tracks the time taken to process each batch and prints progress updates at regular intervals.
        After uploading all the edges, the function prints a summary of the total time taken and estimates the time required to upload 100 million edges.
    '''
    def insertRelations(tx, relations):
        query = f"""
        UNWIND $batch AS row
        MATCH (w1:WORD {{id: row.id1}}), (w2:WORD {{id: row.id2}})
        CREATE (w1)-[:{edge_name} {{weight: row.weight}}]->(w2)
        """
        tx.run(query, batch=relations)

    def processBatch(batch):
        start = time.time()
        session.execute_write(insertRelations, batch)
        return time.time() - start, len(batch)
    
    line_count = sum(1 for _ in open(edges_file, 'r'))
    print(f"Uploading {line_count/1e6:0.0f}e6 edges ({edge_name}) in batches of {batch_size/1e6:0.0f}e6.")

    with open(edges_file, 'r') as edges:
        for _ in range(start_from + 1):
            edges.readline()

        index = start_from
        batch = []
        times = []
        final_size = 0
        most_start = time.time()

        for line in edges:
            index += 1
            content = [int(x) for x in line.split()]
            if len(content) != 3:
                continue
            batch.append({k: v for k, v in zip(["id1", "id2", "weight"], content)})

            # Upload batch
            if len(batch) == batch_size:
                elapsed_time, loaded = processBatch(batch)
                times.append(elapsed_time)
                final_size += loaded
                batch = []

            # Print progress
            if index % alert == 0:
                fElapsed = parseTime(time.time() - most_start)
                print(f"-> Relations ({edge_name}): {index/1e6:0.0f}e6 >> [{fElapsed}].")

        # Upload the last batch
        if batch:
            elapsed_time, loaded = processBatch(batch)
            times.append(elapsed_time)
            final_size += loaded

    # Time Summary
    total_time = sum(times)
    print(f"\n== Uploaded {final_size/1e6:0.0f}e6 edges relations ({edge_name}) in {parseTime(total_time)} ==")

    avg_time_per_million = int(1e8 * total_time / final_size)
    print(f"== Estimated time for 1e8 relations ({edge_name}): {parseTime(avg_time_per_million)} ==")

def saveMST(session, id, edge_name, graph_name='ORIGINAL', rwp='weight'):
    '''
    Saves the Minimum Spanning Tree (MST) of a graph to the Neo4j database.

    Args:
        session (neo4j.Session): The Neo4j session object.
        id (int): The ID of the node to use as the source for the MST.
        edge_name (str): The name of the edge type to use for the MST.
        graph_name (str, optional): The name of the graph to get its MST. Defaults to 'ORIGINAL'.
        rwp (str, optional): The name of the weight property to use for the MST. Defaults to 'weight'.

    Description:
        This function saves the Minimum Spanning Tree (MST) of a graph to the Neo4j database.
        It first projects the graph into a new graph with the specified edge type and weight property.
        Then, it uses the GDS library to calculate the MST of the graph and writes it to the database.
    '''
    # Project the graph
    print(f"Projecting graph ...", end=" ")
    new_ename = f'MINST {edge_name}'  # New relationship name

    # Check if the graph already exists
    result = session.run(f"RETURN gds.graph.exists('{graph_name}') AS exists")
    if result.single()['exists']:
        # Drop the graph if it already exists
        session.run(f"CALL gds.graph.drop('{graph_name}', false)")

    # Project the graph with the specified relationship
    session.run(f"""
    MATCH (source:WORD)-[r:{edge_name}]->(target:WORD)
    RETURN gds.graph.project(
     '{graph_name}', source, target,
    {{relationshipProperties: r {{ .weight }} }},
    {{ undirectedRelationshipTypes: ['*']     }}
    )
    """)
    print(f"SUCCESS")

    # Create the MST
    print(f"Creating MINST ...", end=" ")
    start = time.time()
    
    # Calculate the MST using the GDS library
    result = session.run(f"""
    MATCH (n:WORD {{id: {id}}})
    CALL gds.spanningTree.write('{graph_name}',{{
        sourceNode: n,
        relationshipWeightProperty: '{rwp}',
        writeProperty: 'write{rwp}',
        writeRelationshipType: '{new_ename}'
        }})
    YIELD effectiveNodeCount
    RETURN effectiveNodeCount;
    """)
    total_time = time.time()-start
    print(f"SUCCESS")
    fElapsed = parseTime(total_time)
    

    # Print information about the MST
    print("MST saved:")
    print(f"\t* <{fElapsed}>")
    print(f"\t* {int(result.single()['effectiveNodeCount'])} nodes.")
    print(f"\t* Original relationship name: '{edge_name}'")
    print(f"\t* New relationship name: '{new_ename}'")
    print(f"\t* Weight property '{rwp}'.")

def runRandomWalks(session, params):
    print(f"Projecting graph ...", end = " ")

    result = session.run(f"RETURN gds.graph.exists('{params['graph_name']}') AS exists")

    if result.single()['exists']:
        session.run(f"CALL gds.graph.drop('{params['graph_name']}')")

    session.run(f"""
    MATCH (source:WORD)-[r:{params['edge_name']}]->(target:WORD)
    RETURN gds.graph.project('{params['graph_name']}', source, target,
    {{ relationshipProperties: r {{ .{params['rwp']} }} }},
    {{ undirectedRelationshipTypes: ['*'] }}
    )
    """)

    print(f"SUCCESS")
    print(f"Generating Random Walks ...", end = " ")
    start = time.time()
    
    result = session.run(f"""
    CALL gds.randomWalk.stream('{params['graph_name']}',
    {{
        walkLength: {params['walk_length']},
        walksPerNode: {params['walks_per_node']},
        relationshipWeightProperty: '{params['rwp']}',
        randomSeed: {params['seed']},
        concurrency: 2
    }}
    )
    YIELD nodeIds
    RETURN nodeIds
    """)

    total_time = time.time()-start
    print(f"SUCCESS")
    fElapsed = parseTime(total_time)

    print("Random Walks saved:")
    print(f"\t* <{fElapsed}>")
    
    for record in result:
        node_ids = record["nodeIds"]
    
        for node_id in node_ids:
            query =  """
            YIELD nodeIds
            UNWIND nodeIds AS nodeId
            MATCH (node) WHERE id(node) = nodeId
            RETURN coalesce(node.name, node.title) AS node
            """
            node_result = session.run(f"""
                MATCH (w:WORD) WHERE elementId(w) = {node_id} 
                RETURN w.word AS word""",
            )

            # Print results
            for node_record in node_result:
                print(f"{node_record['word']} -> ", end = "")
        print("",end = "|")
    
def countRelationships(session, edge_name=""):
    '''
    Counts the total number of relationships of a specific type in the graph.

    Args:
        session (neo4j.Session): The Neo4j session object.
        edge_name (str, optional): The name of the relationship type to count. Defaults to an empty string, which means all relationships will be counted.

    Returns:
        int: The total number of relationships of the specified type.

    Description:
        This function uses a Cypher query to count the total number of relationships of a specific type in the graph.
        If no edge name is provided, it will count all relationships in the graph.
    '''
    # Add a colon to the edge name if there is a relation type to filter
    if edge_name:
        edge_name = ": " + edge_name

    # Create the Cypher query to count the relationships
    query = f"""
    MATCH ()-[r{edge_name}]->() 
    RETURN count(r) AS total_relationships
    """
    
    def _execute_count(tx):
        result = tx.run(query)
        record = result.single() 
        # Return the total number of relationships
        return record["total_relationships"]

    return session.execute_read(_execute_count)

def cleanDatabase(session, batch_size = int(1e3)):
    def rmvIndex():
        """
        Verifica si existe un índice en el atributo `id` de los nodos `WORD` y lo elimina si está presente.
        """

        query_check_index = """
        SHOW INDEXES
        """

        try:
            result = session.run(query_check_index)
            index_names = []

            for record in result:
                type, labels, prop = record["entityType"], record["labelsOrTypes"], record["properties"]
                if labels is None or prop is None or type != "NODE":
                    continue

                if "WORD" in labels and "id" in prop:
                    index_names.append(record["name"])
            
            if index_names:
                for index_name in index_names:
                    session.run(f"DROP INDEX {index_name}")

        except Exception as e:
            print(f"ERROR: {e}")
    
    # Delete Indexes
    rmvIndex()

    # Delete Relations
    clearAllRelationships(session)

    # Delete Nodes
    print("Erasing all nodes .... ", end="")
    session.run("MATCH (n) DELETE n")
    print("Finished.")

    # Print Summary
    print("Database successfully cleaned.")

def clearAllRelationships(session, batch_size=int(1e6)):
    print("Erasing all relations ", end="")
    while True:
        print(".", end="")
        result = session.run(
            """
            MATCH ()-[r]->()
            WITH r LIMIT $batch_size
            DELETE r
            RETURN COUNT(r) AS deleted
            """, batch_size=batch_size
        )
        deleted = result.single()["deleted"]
        if deleted == 0:
            break
    print(" Finished.")