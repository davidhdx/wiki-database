from modules.steps import initProcess, stepDecompress
from multiprocessing import Pool

initProcess()

pendings = [[f"enwiki-{i}.txt"] for i in range(1, 67) if i != 17]
workers = len(pendings)

with Pool(workers) as p:
    p.map(stepDecompress, pendings)
    
