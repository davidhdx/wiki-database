from modules.cleaning import fullCleaning
from modules.utils import parseTime
import xml.sax
import time

class TextOnlyHandler(xml.sax.ContentHandler):
    """
    Class to handle the content of an XML file and extract the text from "text" elements.
    """
    def __init__(self, filename):
        """
        Initializes the handler with a prefix for the output file.
        
        Args:
            filename (str): Name for the output file.
        """
        self.current_tag = None
        self.filename = filename 
        self.output_file = None
        self.current_text = ""
        self.cleaned_text = None
        self.processed_pages = 0
        self.open_new_file()  # Opens the first file
        self.start = time.time()

    def open_new_file(self):
        """
        Closes the current file and opens a new one with a counter in the name.
        """
        if self.output_file:
            self.output_file.close()
        filename = f"{self.filename}.txt"
        self.output_file = open(filename, "w", encoding="utf-8")

    def startElement(self, name, attrs):
        """
        Detects the opening of an element in the XML file.
        
        Args:
            name (str): Name of the element.
            attrs (dict): Attributes of the element.
        """
        if name == "text":
            self.current_tag = "text"

    def characters(self, content):
        """
        Processes the content of an element in the XML file.
        
        Args:
            content (str): Content of the element.
        """
        try:
            if self.current_tag == "text" and content.strip():
                self.current_text += content.strip() + " "
        except Exception as e:
                print(f"ERROR: An error occurred on page {self.processed_pages+1}: {e}")

    def resetBuffer(self):
        """
        Resets the current text buffer.
        """
        self.current_tag = None
        self.current_text = ""
        self.cleaned_text = None

    def endElement(self, name):
        """
        Detects the closing of an element in the XML file.
        
        Args:
            name (str): Name of the element.
        """
        if name == "text":
            self.processed_pages += 1
            try:
                # Skips articles that start with "#redirect"
                if not self.current_text.lower().startswith("#redirect"):
                    # Cleans the text using the fullCleaning function
                    self.cleaned_text = fullCleaning(self.current_text)
                    
                    if len(self.cleaned_text) > 0:                  
                        self.output_file.write(self.cleaned_text + "\n")
            except Exception as e:
                print(f"ERROR: An error occurred on page {self.processed_pages}: {e}")

            self.resetBuffer()

    def close(self):
        """
        Closes the currently open file.
        """
        if self.output_file:
            self.output_file.close()
        self.error_file.close()

def parseXML(file_in, file_path): 
    """
    Processes an XML file and extracts the text from "text" elements.
    
    Args:
        file_in (str): Path to the input XML file.
        file_path (str): Path for the output file.
    
    Returns:
        list: List of generated files.
    """
    parser = xml.sax.make_parser()

    start = time.time()
    handler = TextOnlyHandler(file_path)
    parser.setContentHandler(handler)
    parser.parse(file_in)

    generated_files = handler.output_files
    worked_lines = handler.i
    handler.close()

    fElapsed = parseTime(time.time()-start)
    print(f"SUCCESS: {worked_lines} cleaned articles were saved in: {file_path}")
    print(f"==Execution time: {fElapsed}==")
    
    return generated_files