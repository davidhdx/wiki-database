from modules.graph import readVocab, storeRelations
from modules.utils import initTokenizer
from modules.steps import initProcess, getStage
from multiprocessing import Pool
import os

initProcess()

vocab = "/home/est_licenciatura_david.delarosa/practicas_profesionales/vocabs/all_vocab.txt"
cleaned_files = [os.path.join(getStage("cleaned"), f"enwiki-{i}.txt") for i in range(1, 67) if i != 17]

relation_types = ["DOT X", "ADJACENT X", "ARTICLE X"]+[f"DISTANCE {i}" for i in range(3, 10, 2)]+[f"WINDOW {i}" for i in range(3, 10, 2)]

tokenizer = initTokenizer()

V = readVocab(vocab)

workers = 5
args = []
for type in relation_types:
    filename = type.replace(" ", "_")
    relations = os.path.join(getStage("graph"), f"{filename}.txt")
    args.append((cleaned_files, relations, V, tokenizer, type))

with Pool(workers) as p:
    print(p.map(storeRelations, args))


