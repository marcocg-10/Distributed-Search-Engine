import re
import unicodedata

# Conjunto de palabras muy frecuentes en inglés que
# no aportan significado relevante en una búsqueda.
STOPWORDS = {
    "the", "and", "for", "are", "with", "that", "this", "from", "was", "were",
    "has", "have", "had", "not", "but", "you", "your", "its", "his", "her",
    "they", "them", "their", "into", "about", "than", "then", "also", "been",
    "can", "may", "such", "when", "where", "which", "while", "who", "what",
    "how", "why", "all", "any", "one", "two", "more", "most", "other",
    "some", "each", "many", "much", "very", "will", "would", "could",
    "should", "there", "these", "those", "between", "because", "through",
    "over", "under", "after", "before", "during", "within", "without",
    "is", "a", "an", "of", "to", "in", "on", "by", "as", "at", "or",
    "it", "be", "if"
}

# Expresión regular usada para identificar términos válidos.
#
# Patrón:
#
# [a-zA-Z]
#     La palabra debe comenzar con una letra.
#
# [a-zA-Z0-9_]{2,}
#     Debe continuar con al menos dos caracteres más.
TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9_]{2,}")


def normalize_text(text: str) -> str:
    """
    Normaliza el texto antes de tokenizarlo.

    Ejemplos:
    "Artificial Intelligence"
        -> "artificial intelligence"
    """

    if text is None:
        return ""

    text = str(text).lower()

    text = unicodedata.normalize("NFD", text)

    text = "".join(
        ch
        for ch in text
        if unicodedata.category(ch) != "Mn"
    )

    return text

def tokenize(text: str) -> list[str]:
    """
    Función principal de preprocesamiento de texto.

    Esta función transforma un documento de texto libre
    en una lista de términos que podrán ser indexados.

    Ejemplo:

    Entrada:
        "Artificial Intelligence and Machine Learning"

    Después de normalizar:
        "artificial intelligence and machine learning"

    Tokens encontrados:
        [
            "artificial",
            "intelligence",
            "and",
            "machine",
            "learning"
        ]

    Después de eliminar stopwords:
        [
            "artificial",
            "intelligence",
            "machine",
            "learning"
        ]
    """

    # Normalizar el texto.
    text = normalize_text(text)

    # Extraer tokens usando la expresión regular.
    tokens = TOKEN_RE.findall(text)

    clean_tokens = []

    # Filtrar tokens irrelevantes.
    for token in tokens:

        # Eliminar stopwords.
        if token not in STOPWORDS and len(token) >= 3:
            clean_tokens.append(token)

    return clean_tokens