from steps import initProcess, stepDecompress
import concurrent.futures

initProcess()

pendings = [f"enwiki-{i}.txt" for i in range(1, 67) if i != 17]
workers = 8

with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
    for archive in pendings:
        executor.submit(stepDecompress, [archive])


    
