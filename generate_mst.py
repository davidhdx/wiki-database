from modules.database import saveMST, URI, AUTH
from modules.steps import getStage, initProcess
from modules.graph import bestId
from neo4j import GraphDatabase
import os

vocab = "/home/est_licenciatura_david.delarosa/practicas_profesionales/vocabs/all_vocab.txt"

relation_types = ["DOT X", "ADJACENT X", "ARTICLE X"]+[f"DISTANCE {i}" for i in range(3, 10, 2)]+[f"WINDOW {i}" for i in range(3, 10, 2)]
filenames = [os.path.join(getStage("graph"), f"{r.replace(' ', '_')}.txt") for r in relation_types]

mapping = {"X": "", "3": "THREEWORDS", "5": "FIVEWORDS", "7": "SEVENWORDS", "9": "NINEWORDS", "11": "ELEVENWORDS"}
edges = [r.split()[0]+mapping[r.split()[1]] for r in relation_types]

initProcess()
with GraphDatabase.driver(URI, auth=AUTH, max_connection_lifetime=30*3600, connection_timeout=60*100) as driver:
    with driver.session() as session:
        print("Database Connected Successfully")
        
        # Generate Minimum Spanning Tree
        for filename, edge_name in zip(filenames, edges):
            best_id = bestId(filename)
            saveMST(session, best_id, edge_name)


