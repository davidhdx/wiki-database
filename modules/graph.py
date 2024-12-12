from collections import Counter
from modules.utils import parseTime
from itertools import combinations
from collections import defaultdict
import time
import re

__UNK = "<unk>"

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
    '''
    Finds the most common ID in a text file.

    Args:
        file_path (str): The path to the text file.

    Returns:
        str: The most common ID in the file.
    '''
    first_values = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            values = line.strip().split()

            if values:
                first_values.append(values[0])

    freq_counter = Counter(first_values)
    most_common_value = freq_counter.most_common(1)[0][0] 
    return most_common_value

def storeRelations(args):
    '''
    Stores the relationships between words in a file.

    Args:
        args (tuple): A tuple containing the following elements:
            - files (list): A list containing the text files to be processed
            - edges (strign): A string that list the output file for the relationships.
            - vocab (dict): A dictionary containing the words and their IDs.
            - tokenizer (func): A function that tokenizes a text into words.
            - type (str): A string indicating the type of relationship to be stored.

    Returns:
        None
    '''
    def orderRelation(a, b):
        # Orders two IDs so that the smaller one is first.
        return (min(a, b), max(a, b))

    def articleRelation(vocab, doc):
        # Calculates the relationships between words in an article.
        vocab_lookup = {word: vocab[word][0] for word in doc}
        return [
            orderRelation(vocab_lookup[w1], vocab_lookup[w2])
            for w1, w2 in combinations(doc, 2)
        ]

    def dotRelation(vocab, doc):
        # Calculates the relationships between words in a text that ends with a period.
        vocab_lookup = {word: vocab[word][0] for word in doc}
        relations = []
        sentence = []
        append_sentence = sentence.append
        extend_relations = relations.extend

        for word in doc:
            append_sentence(word)
            if "." in word:
                extend_relations(
                    orderRelation(vocab_lookup[w1], vocab_lookup[w2])
                    for w1, w2 in combinations(sentence, 2)
                )
                sentence.clear()

        return relations

    def windowRelation(vocab, doc, window):
        # Calculates the relationships between words in a text within a window of a certain size.
        vocab_lookup = {word: vocab[word][0] for word in doc}
        n = len(doc)
        relations = []
        extend_relations = relations.extend
        
        for i in range(n-window+1):
            extend_relations(
                orderRelation(vocab_lookup[w1], vocab_lookup[w2])
                for w1, w2 in combinations(doc[i:i+window], 2)
                )

        return relations

    def distanceRelation(vocab, doc, dist):
        # Calculates the relationships between words in a text at a certain distance.
        vocab_lookup = {word: vocab[word][0] for word in doc}
        return [
            orderRelation(vocab_lookup[doc[i]], vocab_lookup[doc[i+dist-1]])
            for i in range(len(doc)-dist+1)
        ]

    def adjacentRelation(vocab, doc):
        # Calculates the relationships between adjacent words in a text.
        vocab_lookup = {word: vocab[word][0] for word in doc}
        return [
            orderRelation(vocab_lookup[doc[i - 1]], vocab_lookup[doc[i]])
            for i in range(1, len(doc))
        ]

    def hashRelations(relations, vocab, doc):
        # Stores the relationships between words in a dictionary.
        new_relations = []
        masked_doc = [word if word in vocab else __UNK for word in doc]

        # Switch the function to use for generating the relations
        info = type.split()
        if info[0] == "DOT":
            new_relations = dotRelation(vocab, masked_doc)
        if info[0] == "ADJACENT":
            new_relations = adjacentRelation(vocab, masked_doc)
        if info[0] == "DISTANCE":
            new_relations = distanceRelation(vocab, masked_doc, int(info[1]))
        if info[0] == "WINDOW":
            new_relations = windowRelation(vocab, masked_doc, int(info[1]))
        if info[0] == "ARTICLE":
            new_relations = articleRelation(vocab, masked_doc)
        
        # Calculating frequencies of the relations
        for r in new_relations:
            relations[r] += 1

        return relations
    
    files, edges, vocab, tokenizer, type = args
    
    # Print number of relations to upload
    print(f">> Counting ({type}) relations.")

    start = time.time()

    # Using a defaultdict(int) to set new keys with value 0
    relations = defaultdict(int)
    
    i=0
    # Processing every file
    for corpus in files:
        i += 1
        with open(corpus, 'r', encoding='utf-8') as corpus:
            print(f"Working ({type}) relation in {i}-th file.")
            # Getting relations contained in every article
            for linea in corpus:
                relations = hashRelations(relations, vocab, list(tokenizer(linea.strip())))
        # Print progress
        fElapsed = parseTime((time.time() - start)/i)
        print(f"=={i} files used in ({type}) relation. Avg time {fElapsed}==")

    # Print Elapsed Time
    fElapsed = parseTime(time.time() - start)
    
    # Save all relations in a txt file
    with open(edges, 'w') as edges_file:
        i = 0
        for k in relations.keys():
            id1, id2 = k
            i += 1
            weight = relations[(id1, id2)]
            edges_file.write(f"{id1} {id2} {weight}\n")

    # Print Summary
    print(f"=={i} relations ({type}) founded in {fElapsed}==")
    fElapsed = parseTime(time.time() - start)
    print(f"=={i} relations ({type}) stored in {fElapsed}==")