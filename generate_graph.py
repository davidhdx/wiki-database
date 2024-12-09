import os
from utils import initTokenizer
from graph import readVocab, storeRelations
from steps import initProcess, getStage
import concurrent.futures

initProcess()

vocab = "/home/est_licenciatura_david.delarosa/practicas_profesionales/vocabs/all_vocab.txt"
cleaned_files = [os.path.join(getStage("cleaned"), f"enwiki-{i}.txt") for i in range(1, 67) if i != 17]

relation_types = ["DOT X", "DIRECT X", "ARTICLE X"]+[f"EXACT {i}" for i in range(3, 12, 2)]+[f"NONEXACT {i}" for i in range(3, 12, 2)]
tokenizer = initTokenizer()

V = readVocab(vocab)

workers = 7

with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
    for type in relation_types:
        filename = type.replace(" ", "_")
        relations = os.path.join(getStage("graph"), f"{filename}.txt")
        executor.submit(storeRelations, (cleaned_files, relations), V, tokenizer, type)



