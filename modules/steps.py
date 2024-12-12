from modules.download import setDownloadFolder, fitMapping
from modules.utils import getRaw, initTokenizer, delete
from modules.vocabulary import fitVocabulary
from modules.parse_wm import parseXML
import bz2
import os

__FOLDER = r'/home/est_licenciatura_david.delarosa/practicas_profesionales'

__ALL_TEXTS = os.path.join(__FOLDER, r"log/all_texts.txt")
__WKD_TEXTS = os.path.join(__FOLDER, r"log/completed.txt")

__STEPS = ["compressed", "decompressed", "cleaned", "vocabs", "graph"]
__STAGES = {n : os.path.join(__FOLDER, n) for n in __STEPS}
__TOKENIZER = None
__MAP = None

def initProcess():
    global __MAP

    for stage in __STAGES.values():
        os.makedirs(stage, exist_ok=True)

    for log in [__ALL_TEXTS, __WKD_TEXTS]:
        if not os.path.exists(log):
            with open(log, 'w') as file:
                file.write("")  

    setDownloadFolder(__STAGES["compressed"])
    __MAP = fitMapping(__ALL_TEXTS)

def mapping(filename):
    if filename in __MAP:
        return __MAP[filename]
    return None

def getStage(name):
    return __STAGES[name]

def extractFrom(bz2_file, extracted_file, k=1):
    with bz2.open(bz2_file, 'rb') as input:
        with open(extracted_file, 'wb') as output:
            while True:
                chunk = input.read(k * 1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)

def addCompleted(filename):
    filename = getRaw(filename, ".bz2")
    with open(__WKD_TEXTS, "a+") as completed:
        completed.write(f"{filename}\n")

def stepVocab(filename, extension = ".txt"):
    global __TOKENIZER

    if filename == "*":
        curr_folder = __STAGES["cleaned"]
        queue = [os.path.join(curr_folder, f) for f in os.listdir(curr_folder) \
                 if os.path.isfile(os.path.join(curr_folder, f))]  
    elif isinstance(filename, str):
        queue = [filename]
    elif isinstance(filename, list):
        queue = filename

    if not queue:
        stepVocab("*")

    next_folder = __STAGES["vocabs"]
    __TOKENIZER =  __TOKENIZER or initTokenizer()

    for cleaned_file in queue:
        print(f"START VOCABULARY")
        vocab_file = os.path.join(next_folder, getRaw(cleaned_file, extension))
        if os.path.exists(vocab_file):
            delete(vocab_file)
        fitVocabulary(cleaned_file, vocab_file, __TOKENIZER)
        addCompleted(vocab_file)
        
def stepClean(filename):
    if filename == "*":
        curr_folder = __STAGES["decompressed"]
        queue = [os.path.join(curr_folder, f) for f in os.listdir(curr_folder) \
                 if os.path.isfile(os.path.join(curr_folder, f))]
    else:
        queue = [filename]

    if not queue:
        stepVocab("*")

    next_folder = __STAGES["cleaned"]
    for decompressed_file in queue:
        print(f"START CLEANER of {decompressed_file}")
        output_path = os.path.join(next_folder, getRaw(decompressed_file))
        cleaned_files = parseXML(decompressed_file, output_path)
        delete(decompressed_file)
        stepVocab(cleaned_files)
        

def stepDecompress(queue, extension = ".xml"):
    next_folder = __STAGES["decompressed"]
    
    if not queue:
        stepClean("*")

    for bz2_file in queue:
        extracted_file = os.path.join(next_folder, getRaw(bz2_file, extension))
        
        if os.path.exists(extracted_file):
            delete(extracted_file)
        extractFrom(bz2_file, extracted_file)
        stepClean(extracted_file)

def processQueue(queue):
    curr_folder = __STAGES["compressed"]

    queue = [os.path.join(curr_folder, f) for f in queue]
    
    stepDecompress(queue)