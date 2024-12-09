import os
from database import uploadNodes, uploadRelations, saveMST, countRelationships, URI, AUTH
from graph import readVocab, bestId
from steps import getStage, initProcess
from neo4j import GraphDatabase

vocab = "/home/est_licenciatura_david.delarosa/practicas_profesionales/vocabs/enwiki-10-top.txt"

relation_types = ["DOT X", "DIRECT X", "ARTICLE X"]+[f"EXACT {i}" for i in range(3, 12, 2)]+[f"NONEXACT {i}" for i in range(3, 12, 2)]
filenames = [os.path.join(getStage("graph"), f"{r.replace(' ', '_')}.txt") for r in relation_types]

mapping = {"X": "", "3": "THREEWORDS", "5": "FIVEWORDS", "7": "SEVENWORDS", "9": "NINEWORDS", "11": "ELEVENWORDS"}
edges = [r.split()[0]+mapping[r.split()[1]] for r in relation_types]

initProcess()
with GraphDatabase.driver(URI, auth=AUTH, max_connection_lifetime=30*3600, connection_timeout=60*100) as driver:
    with driver.session() as session:
        print("SUCCESS Connecting Database")

        # Subir los tokens como nodos en la base de datos
        V = readVocab(vocab)
        uploadNodes(session, [{"word":w, "id":V[w][0]} for w in V.keys()])

        # Subir las relaciones encontradas con una etiqueta diferente segun el tipo de relacion
        for filename, edge_name in zip(filenames, edges):
            uploadRelations(filename, session, edge_name)
            print(f'>> {countRelationships(session, edge_name)} edges in the {edge_name} graph.')

        # Generar los arboles minimos de expansión de cada tipo de relacion
        for filename, edge_name in zip(filenames, edges):
            best_id = bestId(filename)
            saveMST(session, best_id, edge_name)


