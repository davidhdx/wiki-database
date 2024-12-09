from collections import defaultdict
from utils import parseTime
import mmap
import time

def fitVocabulary(input_path, output_path, tokenizer):
    start = time.time()
    with open(input_path, 'r+', encoding='utf-8') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        article_count = sum(1 for _ in iter(mm.readline, b""))
        mm.seek(0)  

        word_data = defaultdict(lambda: [0])
        
        for idx, linea in enumerate(iter(mm.readline, b"")):
            content = linea.decode('utf-8', errors='ignore').strip()
            word_frequencies = defaultdict(int)
            tokens = tokenizer(content)
            
            for word in tokens:
                word_frequencies[word] += 1
            
            for word, freq in word_frequencies.items():
                word_data[word][0] += freq
                word_data[word].append((idx, freq))

        mm.close()  # Cerramos mmap una vez que terminamos de procesar el archivo

    with open(output_path, 'w') as output:
        output.write(f"{article_count}\n")
        sorted_word_data = sorted(word_data.items(), key=lambda item: item[1][0], reverse=True)
        for word, data in sorted_word_data:
            output.write(f"{word} {' '.join(map(str, data))}\n")

    fElapsed = parseTime(time.time()-start)
    print(f"SUCCESS: Vocabulary of {input_path} saved in {fElapsed}.")
