import unidecode
import re

def depure(texto):
    dump = ["en.wikipedia.orgwindex.phptitlespeciallog",
    "mnmhttpsrkd.nlenexploreartistssearchsimple",
    "sortkeysholdingscoun",
    "rkdhttpviaf.orgviafsearchquerylocal.personalnamesall",
    "id",
    "dykc",
    "httpsarchive.today",
    "populationfootnotes",
    "populationdensitykm",
    "medaltemplates",
    "ltd",
    "ffff"]
    for d in dump:
        texto = texto.replace(d, " ")

    texto = re.sub(r"\s(\.|\s)*\s", '. ', texto)
    texto = re.sub(r"\s(\.|\s)*\s", '. ', texto)
    texto = re.sub(r"\.[\s]*,\s", ', ', texto)
    
    texto = re.sub(r"\w*\.jpg", ' ', texto)
    texto = re.sub(r"\w*\.png", ' ', texto)
    texto = re.sub(r"\w*\.svg", ' ', texto)
    texto = re.sub(r"\w*\.html", ' ', texto)
    texto = re.sub(r"\w*\.pdf", ' ', texto)
    texto = re.sub(r"\w*\.orgweb", ' ', texto)
    texto = re.sub(r"\w*\.uk", ' ', texto)
    texto = re.sub(r"usercoibot\w*", ' ', texto)
    texto = re.sub(r"wikipedia\w+", ' ', texto)
    texto = re.sub(r"textalign\w+", ' ', texto)
    texto = re.sub(r"nbsp\w*", ' ', texto)
    texto = re.sub(r"category\w+", ' ', texto)
    texto = re.sub(r"shading\w+", ' ', texto)
    texto = re.sub(r"[\w]*\.xsl", ' ', texto)
    texto = re.sub(r"\.[\.\s,]*", '. ', texto)
    texto = re.sub(r",[\.\s,]*", ', ', texto)
    texto = re.sub('\n', ' ', texto)
    texto = re.sub(r'\s{2,}', ' ', texto)

    return texto.strip()

def fullCleaning(texto):
    texto = unidecode.unidecode(texto)
    texto = re.sub(r"==[\s^=]*?References\s*?==\n.*<EOA>", '<reference links>', texto+"<EOA>", flags=re.DOTALL)

    
    #filtrar secciones basura
    texto = re.sub(r'===\[\[.*?\]\]===', '<dump section>', texto, flags=re.DOTALL)
    #filtrar secciones basura
    texto = re.sub(r'===.*?===', '<subsection>', texto, flags=re.DOTALL)
    #filtrar secciones
    texto = re.sub(r'==.*?==', '<section>', texto, flags=re.DOTALL)
    #eliminar secciones basura
    texto = re.sub(r'<dump section>\n.*<EOA>', '', texto, flags=re.DOTALL)

    #filtrar comandos de caracteres
    texto = texto.replace('&lt;', '<')
    texto = texto.replace('&gt;', '>')
    texto = texto.replace('&amp;', '&')
    texto = texto.replace('&quot;', '"')
    texto = texto.replace('#ifeq:', '<conditional>')
    texto = texto.replace('&nbsp;', '<nonbreaking>')
    texto = texto.replace('&n#124', '|')
    texto = texto.replace('e.g.', '<latin loc>')

    #simplicar etiquetas complejas
    texto = re.sub(r'<ref.*?</ref>', '<refs>', texto)
    texto = re.sub(r'<span.*?</span>', '<spans>', texto)
    texto = re.sub(r'<sup.*?</sup>', '<sups>', texto)

    #existen categorias
    texto = re.sub(r'\[\[\s*Category\s*:\s*[\w\|\s:<>\-]+\]\]', "<category>", texto)

    texto = re.sub(r'\'\'\'\'\'([.^\'\n]*?)\'\'\'\'\'', r'\1', texto, flags=re.DOTALL)
    texto = re.sub(r'\'\'\'\'([.^\'\n]*?)\'\'\'\'', r'\1', texto, flags=re.DOTALL)
    texto = re.sub(r'\'\'\'([.^\'\n]*?)\'\'\'', r'\1', texto, flags=re.DOTALL)
    texto = re.sub(r'\'\'([.^\'\n]*?)\'\'', r'\1', texto, flags=re.DOTALL)
    texto = re.sub(r'\'([.^\'\n]*?)\'', r'\1', texto, flags=re.DOTALL)

    # texto = re.sub(r'<.*?>', ' ', texto)
    
    # texto = re.sub(r'<ref.*?&lt;/ref&gt;', '', texto, re.DOTALL)
   
    #hasta 6 aperturas de llaves sin absolutos
    texto = re.sub(r'\{\{\{([\w\s/\-^\{]*)\}\}\}', r"\1", texto)
    texto = re.sub(r'\{\{([\w\s/\-^\{]*)\}\}', r"\1", texto)
    texto = re.sub(r'\{([\w\s/\-^\{]*)\}', r"\1", texto)

    #hasta 6 aperturas de corchetes sin absolutos
    texto = re.sub(r'\[\[\[([\w\s/()\-]*)\]\]\]', r"\1", texto)
    texto = re.sub(r'\[\[([\w\s/()\-]*)\]\]', r"\1", texto)
    texto = re.sub(r'\[([\w\s/()\-]*)\]', r"\1", texto)    

    #dobles aperturas con exactamente un absoluto
    texto = re.sub(r'\[\[[\w\s()/:<>,\-]+\|([\w\s:<>,\-]+)\]\]', r"\1", texto)
    texto = re.sub(r'\{\{[\w\s():/<>,\-]+\|[\w\s:<>,\-]+\}\}', "<semilink>", texto)

    #condicionales with no-newline
    texto = re.sub(r'\{\{[\s^\n]*?<conditional>[\s\w-]*?\|[\s\w-]*?\|[\s\w<>\'-]*?\|[\s\w<>\'-]*?\}\}', 
                   "<condtional nonewline>", texto)
    
    #audios
    texto = re.sub(r'\{\{listen\|.*?\}\}', "<audio>", texto, flags=re.DOTALL)

    #existen condicionales con audios
    texto = re.sub(r'\{\{[\s^\n]*?<conditional>[\s\w-]*?\|[\s\w-]*?\|[\s\w<>\'-]*?\|[\s\w<>\'-]*?\}\}', 
                   "<condtional with audio>", texto)    

    #para las tablas en linea o con saltos de linea
    texto = re.sub(r'\{\{[^}\n]*\|[^}\n]*\}\}[\n]*', '<tabular nonewline>', texto)
    texto = re.sub(r'\{\{[^}]*\|[^}]*\}\}[\n]*', '<tabular>', texto)
    texto = re.sub(r'\{\|[^}\n]*\|[^}\n]*\|\}[\n]*', '<chart nonewline>', texto)
    texto = re.sub(r'\{\|[^}]*\|[^}]*\|\}[\n]*', '<chart>', texto)
    
    
    #textos en negritas
    texto = re.sub(r';(.*?):', r'\1', texto, re.DOTALL)
    # # texto = re.sub(r'\:\n', ': ', texto, flags=re.DOTALL)
    texto = re.sub(r'\*\s(.*?)', r'\1, ', texto, re.DOTALL)
    texto = re.sub(r"\s*\([^(]*\)", '', texto)

    
    #eliminar comentarios
    texto = re.sub(r'<!--.*?-->', '<comments>', texto, flags=re.DOTALL)

    #eliminar etiquetas
    texto = re.sub(r'<.*?>', ' ', texto, flags=re.DOTALL)

    texto = re.sub(r'\d[\d\.]+', '<number>', texto)
    texto = re.sub(r'<number>:<number>, <number> (January|February|March|April|May|June|July|August|September|October|November|December) <number>', '<date>', texto)
    texto = re.sub(r'<number> (January|February|March|April|May|June|July|August|September|October|November|December) <number>', '<date>', texto)
    #eliminar etiquetas
    texto = re.sub('<number>|<date>', ' ', texto, flags=re.DOTALL)

    #eliminar saltos de linea 
    texto = re.sub(r'[\n]+', " ", texto)

    #coherencia
    texto = texto.replace(': ,', ': ')
    texto = texto.replace(':', ' ')
    
    #solo caracteres normales
    texto = re.sub(r'[^a-zA-ZáéíóúÁÉÍÓÚüÜñÑ,.\s]', '', texto)
    texto = re.sub(r'[_-]', '', texto)
    texto = re.sub(r'\.(\s\.)+', '.', texto)
    texto = re.sub(r',(\s,)+', ',', texto)
    
    texto = re.sub(r'[\s]+', ' ', texto)
    texto = re.sub(r'[\.]+', '.', texto)
    texto = re.sub(r'[\,]+', ',', texto)

    texto = texto.replace(' ,', ',')
    texto = texto.replace('.,', '.')
    texto = texto.replace(',.', '.')

    texto = re.sub(r'\d', '', texto)
    texto = re.sub(r',(\w)', r', /1', texto)
    texto = re.sub(r'\b\w*(http|www|\.\w)\w*\b', ' ', texto)
    texto = re.sub(r'[\n]+', " ", texto)
    texto = re.sub(r'[\s]+', ' ', texto)

    
    
    texto = texto.strip()
    texto = texto.lower()

    if not texto:
        return texto
    
    if texto[-1] != ".":
        texto += "."

    return depure(texto)