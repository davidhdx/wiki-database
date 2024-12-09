from nltk.tokenize import word_tokenize
import nltk
import os

__PUNKT = False
__PUNKT_TAB = False

def parseTime(elapsed):
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)

    return f"{hours:02}:{minutes:02}:{seconds:02}"

def splitInGroups(arr, k):
    return [arr[i:i + k] for i in range(0, len(arr), k)]

def initTokenizer():
    global __PUNKT, __PUNKT_TAB

    if not __PUNKT:
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            print("**'punkt' not founded. Downloading...")
            nltk.download('punkt')
        __PUNKT = True

    if not __PUNKT_TAB:
        try:
            nltk.data.find('tokenizers/punkt_tab')
        except LookupError:
            print("**'punkt_tab' not founded. Downloading...")
            nltk.download('punkt_tab')
        __PUNKT_TAB = True
    return word_tokenize

def delete(file_path):
    try:
        os.remove(file_path)
    except FileNotFoundError:
        print(f"ERROR: Not founded.")
    except PermissionError:
        print(f"ERROR: Not access.")
    except Exception as e:
        print(f"ERROR: Unknown error.")

def getRaw(fullpath, extension = ""):
    return os.path.basename(fullpath).split(".")[0]+extension