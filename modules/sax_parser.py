from cleaning import fullCleaning
from datetime import datetime
import xml.sax
import time

class TextOnlyHandler(xml.sax.ContentHandler):
    def __init__(self, file_prefix, max_lines=int(1e7)):
        self.current_tag = None
        self.file_prefix = file_prefix  # Prefijo para los archivos de salida
        self.current_file = None
        self.current_text = ""
        self.cleaned_text = None
        self.i = 0  # Contador de páginas procesadas
        self.line_count = 0  # Contador de líneas en el archivo actual
        self.file_count = 0  # Contador de archivos generados
        self.max_lines = max_lines  # Máximo de líneas por archivo
        self.output_files = []  # Lista de archivos generados
        self.k = int(1e3)
        self.show_text = False
        self.open_new_file()  # Abre el primer archivo
        self.start = time.time()
        self.date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def open_new_file(self):
        """Cierra el archivo actual y abre uno nuevo con un contador en el nombre."""
        if self.current_file:
            self.current_file.close()
        self.file_count += 1
        filename = f"{self.file_prefix}.txt"
        self.current_file = open(filename, "w", encoding="utf-8")
        self.output_files.append(filename)  # Agrega el archivo a la lista
        self.line_count = 0

    def startElement(self, name, attrs):
        if name == "text":
            self.current_tag = "text"

    def characters(self, content):
        try:
            if self.current_tag == "text" and content.strip():
                self.current_text += content.strip() + " "
        except Exception as e:
                print(f"ERROR: Ocurrió un error en la página {self.i+1}: {e}")

    def resetBuffer(self):
        self.current_tag = None
        self.current_text = ""
        self.cleaned_text = None

    def endElement(self, name):
        if name == "text":
            self.i += 1
            try:
                if not self.current_text.lower().startswith("#redirect"):
                    self.cleaned_text = fullCleaning(self.current_text)
                    
                    if len(self.cleaned_text) > 0:
                        self.line_count += 1                        
                        self.current_file.write(self.cleaned_text + "\n")
            except Exception as e:
                print(f"ERROR: Ocurrió un error en la página {self.i}: {e}")

            self.resetBuffer()

    def close(self):
        """Cierra el archivo abierto actualmente."""
        if self.current_file:
            self.current_file.close()
        self.error_file.close()

