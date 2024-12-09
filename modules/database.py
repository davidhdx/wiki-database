from utils import parseTime
import time

URI = "bolt://172.23.3.247:7687"
AUTH = ("neo4j", "12345678")
_BATCH_SIZE = int(1e6)

def uploadNodes(session, nodes, batch_size=_BATCH_SIZE):
    def insertNodes(tx, nodes):
        query = """
        UNWIND $batch AS row
        CREATE (w:WORD {id: row.id, word: row.word})
        """
        tx.run(query, batch=nodes)

    def process_batch(batch):
        session.execute_write(insertNodes, batch)

    print(f"Insertando {len(nodes)} nodos...")
    start = time.time()

    n = len(nodes)
    batches = [nodes[i:min(i + batch_size, n)] for i in range(0, n, batch_size)]
    
    for batch in batches:
        process_batch(batch)

    session.run("""
    CREATE INDEX FOR (w:WORD) ON (w.id)
    """)

    fElapsed = parseTime(time.time() - start)
    print(f"Tiempo para insertar nodos: [{fElapsed}]")

def uploadRelations(edges_file, session, edge_name, batch_size=_BATCH_SIZE, start_from=0, alert=int(1e8)):
    def insertRelations(tx, relations):
        query = f"""
        UNWIND $batch AS row
        MATCH (w1:WORD {{id: row.id1}}), (w2:WORD {{id: row.id2}})
        CREATE (w1)-[:{edge_name} {{weight: row.weight}}]->(w2)
        """
        tx.run(query, batch=relations)

    def processBatch(batch):
        """
        Procesa un lote enviándolo a la base de datos y mide el tiempo requerido.
        """
        start = time.time()
        session.execute_write(insertRelations, batch)
        return time.time() - start, len(batch)
    
    # Contar líneas totales para información al usuario
    line_count = sum(1 for _ in open(edges_file, 'r'))
    print(f"Uploading {line_count:0.2e} edges in batches of {batch_size}.")

    with open(edges_file, 'r') as edges:
        # Avanzar líneas hasta el punto de partida
        for _ in range(start_from + 1):
            edges.readline()

        index = start_from
        batch = []
        times = []
        final_size = 0
        most_start = time.time()

        for line in edges:
            index += 1
            # Procesar contenido de la línea
            content = [int(x) for x in line.split()]
            if len(content) != 3:
                continue
            batch.append({k: v for k, v in zip(["id1", "id2", "weight"], content)})

            # Procesar lote si alcanzamos el tamaño especificado
            if len(batch) == batch_size:
                elapsed_time, loaded = processBatch(batch)
                times.append(elapsed_time)
                final_size += loaded
                batch = []

            # Mostrar progreso cada 'alert' líneas
            if index % alert == 0:
                fElapsed = parseTime(time.time() - most_start)
                print(f"-> Relations: {index // alert}e8 >> [{fElapsed}].")

        # Procesar cualquier lote restante
        if batch:
            # print(f"Procesando último lote en línea {index}.")
            elapsed_time, loaded = processBatch(batch)
            times.append(elapsed_time)
            final_size += loaded

    # Resumen de tiempos
    total_time = sum(times)
    print(f"\n== Uploaded {final_size} relations in {parseTime(total_time)} ==")

    avg_time_per_million = int(1e8 * total_time / final_size)
    print(f"== Estimated time for 100 million relations: {parseTime(avg_time_per_million)} ==")

def cleanDatabase(session):
    # Ejecuta la consulta para eliminar todos los nodos y relaciones
    session.run("MATCH (n) DETACH DELETE n")
    clearAllRelationships(session)
    print("Base de datos limpiada exitosamente.")

def saveMST(session, id, edge_name, graph_name='ORIGINAL', rwp='weight'):
    print(f"Projecting graph ...", end = " ")
    new_ename = f'MINST {edge_name}'

    result = session.run(f"RETURN gds.graph.exists('{graph_name}') AS exists")

    if result.single()['exists']:
        session.run(f"CALL gds.graph.drop('{graph_name}', false)")

    session.run(f"""
    MATCH (source:WORD)-[r:{edge_name}]->(target:WORD)
    RETURN gds.graph.project(
     '{graph_name}', source, target,
    {{relationshipProperties: r {{ .weight }} }},
    {{ undirectedRelationshipTypes: ['*']     }}
    )
    """)

    print(f"SUCCESS")

    print(f"Creating MINST ...", end = " ")
    start = time.time()
    
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
    

    print("MST saved:")
    print(f"\t* <{fElapsed}>")
    print(f"\t* {int(result.single()['effectiveNodeCount'])} nodes.")
    print(f"\t* Original relation name: '{edge_name}'")
    print(f"\t* New relation name: '{new_ename}'")
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
    vocab = params['vocab']
    
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
            # Imprime o guarda los resultados
            for node_record in node_result:
                print(f"{node_record['word']} -> ", end = "")
        print("",end = "|")
    
def countRelationships(session, edge_name=""):
    if edge_name:
        edge_name = ": "+edge_name

    query = f"""
    MATCH ()-[r{edge_name}]->() 
    RETURN count(r) AS total_relationships
    """
    
    def _execute_count(tx):
        result = tx.run(query)
        record = result.single() 
        return record["total_relationships"]

    return session.execute_read(_execute_count)

def clearAllRelationships(session, batch_size=int(1e7)):
    while True:
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
    print("Cleared all relations.")