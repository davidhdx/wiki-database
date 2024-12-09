import requests
import os

__WEBSITE = r"https://dumps.wikimedia.org/enwiki/20240901/"
__DOWNS = None
__NAMES = None

def generateIsomorphicNames(file_list):
    isomorphic_names = {}
    name_counter = {}

    for file in file_list:
        base_name = file.split('.')[0] 
        
        if base_name in name_counter:
            name_counter[base_name] += 1
        else:
            name_counter[base_name] = 1
        
        if name_counter[base_name] > 1:
            new_name = f"{base_name}-{name_counter[base_name]}"
        else:
            new_name = base_name
        
        isomorphic_names[file] = new_name

    return isomorphic_names

def setDownloadFolder(folder):
    global __DOWNS
    __DOWNS = folder

def setMapping(folder):
    with open(folder, "r+") as f:
        pendings = [line.strip() for line in f]
        
    global __NAMES
    __NAMES = generateIsomorphicNames(pendings)

def getMapping():
    return __NAMES

def fitMapping(folder):
    setMapping(folder)
    return getMapping()

def download(filename):
    try:
        url = __WEBSITE + filename
        response = requests.get(url, stream=True)
        response.raise_for_status()

        output = os.path.join(__DOWNS, __NAMES[filename] + ".bz2")
        print(f"Downloading ({filename}) ...")
        with open(output, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return filename

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Unkwown error in ({url})")
        return None

