from modules.sax_parser import TextOnlyHandler
from modules.utils import parseTime
import xml.sax
import time

def parseXML(file_in, prefix_path): 
    parser = xml.sax.make_parser()

    start = time.time()
    handler = TextOnlyHandler(prefix_path)
    parser.setContentHandler(handler)
    parser.parse(file_in)

    generated_files = handler.output_files
    worked_lines = handler.i
    handler.close()

    fElapsed = parseTime(time.time()-start)
    print(f"SUCCESS: {worked_lines} cleaned articles were saved in: {prefix_path}")
    print(f"==Execution time: {fElapsed}==")
    
    return generated_files
