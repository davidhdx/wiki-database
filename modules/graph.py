from collections import Counter
from modules.utils import parseTime
import time
import re

__UNK = "<unk>"

def orderRelation(a, b):
    return (min(a, b), max(a, b))

def articleRelation(vocab, doc):
    n = len(doc)
    relations = []
    for i in range(n-1):
        for j in range(i+1, n):
            relations.append(orderRelation(vocab[doc[i]][0], vocab[doc[j]][0]))

    return relations

def dotRelation(vocab, doc):
    n = len(doc)
    sentence = []
    relations = []
    for i in range(n):
        sentence.append(doc[i])
        if "." in doc[i]:
            for j in range(len(sentence)-1):
                for k in range(j+1, len(sentence)):
                    relations.append(orderRelation(vocab[sentence[j]][0], vocab[sentence[k]][0]))
            sentence = []

    return relations

def windowRelation(vocab, doc, window, type):
    n = len(doc)
    relations = []
    
    for i in range(n - window):

        relations.append(orderRelation(vocab[doc[i]][0], vocab[doc[i+window-1]][0]))
        if type == "NONEXACT":
            for j in range(1, window-1):
                relations.append(orderRelation(vocab[doc[i]][0], vocab[doc[i+j]][0]))

    return relations

def directRelation(vocab, doc):
    return [orderRelation(vocab[doc[i-1]][0], vocab[doc[i]][0]) for i in range(1, len(doc))]

def readVocab(input_path):
    V = {}
    pattern = re.compile(r'\s+')
    V[__UNK] = (0, 0)
    with open(input_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            groups = pattern.split(line.strip())
            w, f = ' '.join(groups[:-1]), groups[-1]
            V[w] = (i+1, int(f))
    
    print("SUCCESS: Vocabulary extracted")
    return V

def reverseVocab(vocab):
    return {v[0]:w for w, v in vocab.items()}

def bestId(file_path):
    first_values = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            values = line.strip().split()

            if values:
                first_values.append(values[0])
    freq_counter = Counter(first_values)
    
    most_common_value = freq_counter.most_common(1)[0][0] 
    
    return most_common_value

def storeRelations(file_paths, vocab, tokenizer, type):
    def hashRelations(relations, vocab, doc):
        new_relations = []
        masked_doc = [word if word in vocab else __UNK for word in doc]

        info = type.split()
        if info[0] == "DOT":
            new_relations = dotRelation(vocab, masked_doc)
        if info[0] == "DIRECT":
            new_relations = directRelation(vocab, masked_doc)
        if info[0] == "EXACT":
            new_relations = windowRelation(vocab, masked_doc, int(info[1]), "EXACT")
        if info[0] == "NONEXACT":
            new_relations = windowRelation(vocab, masked_doc, int(info[1]), "NONEXACT")
        if info[0] == "ARTICLE":
            new_relations = articleRelation(vocab, masked_doc)
        
        for r in new_relations:
            if r not in relations:
                relations[r] = 1
            else:
                relations[r] += 1

        return relations
    
    print(f">> Counting ({type}) relations.")
    
    files, edges = file_paths
    start = time.time()
    relations = {}
    for corpus in files:
        with open(corpus, 'r+', encoding='utf-8') as corpus:
            print(f"Working in {i}-th file.")
            for linea in corpus:
                relations = hashRelations(relations, vocab, list(tokenizer(linea.strip())))

    fElapsed = parseTime(time.time() - start)
    
    with open(edges, 'w') as edges_file:
        i = 0
        for k in relations.keys():
            id1, id2 = k
            i += 1
            weight = relations[(id1, id2)]
            edges_file.write(f"{id1} {id2} {weight}\n")

    print(f"=={i} relations ({type}) founded in {fElapsed}==")
    fElapsed = parseTime(time.time() - start)
    print(f"=={i} relations ({type}) stored in {fElapsed}==")

    edges.close()
