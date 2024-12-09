from modules.vocabulary import processAllVocabs, mergeDicts
from multiprocessing import Pool
import os

_VOCABS = "/home/est_licenciatura_david.delarosa/practicas_profesionales/vocabs/"

file_names = [f"enwiki-{i}.txt" for i in range(1, 67) if i != 17]
file_list = [os.path.join(_VOCABS, f) for f in file_names]
output_file = os.path.join(_VOCABS, f"all_vocab.txt")

with Pool(processes=len(file_list)) as pool:
    results = pool.map(processAllVocabs, file_list)

print("Start merging")
final_frequencies = mergeDicts(results)
print("Finished merging")

i = 0
with open(output_file, "w") as out_file:
    for token, frequency in sorted(final_frequencies.items(), key=lambda x: x[1], reverse=True):
        i += 1
        out_file.write(f"{token} {frequency}\n")
        if i == int(1e6):
            break

