import math
import re
import html
from collections import Counter

def clean_html(text):
    """
    Cleans HTML content from text.
    Decodes entities and removes tags.
    """
    if not text:
        return ""
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Collapse multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def split_sentences(text):
    """
    Splits text into sentences using a robust regex pattern.
    Handles standard punctuation (.?!) and avoids common abbreviations.
    """
    if not text:
        return []
    
    # Regex explanation:
    # (?<!\w\.\w.)    : Negative lookbehind for abbreviations like U.S.
    # (?<![A-Z][a-z]\.) : Negative lookbehind for abbreviations like Dr.
    # (?<=\.|\?|\!)     : Positive lookbehind for sentence enders
    # \s                : Whitespace splitter
    pattern = r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s'
    sentences = re.split(pattern, text)
    return [s.strip() for s in sentences if s.strip()]

def tokenize(text):
    """
    Simple tokenizer: lowercase, remove non-alphanumeric, remove stop words.
    """
    text = text.lower()
    # customized stop words
    stop_words = {
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "with",
        "is", "are", "was", "were", "of", "it", "that", "this", "by", "from", "be",
        "as", "have", "has", "had", "not", "which", "will", "can", "would", "could"
    }
    
    # split by non-alphanumeric
    tokens = re.split(r'[^a-z0-9]+', text)
    return [t for t in tokens if t and t not in stop_words]

def compute_tf_idf(sentences):
    """
    Computes TF-IDF vectors for a list of sentences.
    Returns a list of dicts {token: score}.
    """
    # 1. Tokenize all sentences
    sentence_tokens = [tokenize(s) for s in sentences]
    n_docs = len(sentences)
    if n_docs == 0:
        return []

    # 2. Compute Document Frequency (DF)
    doc_freq = Counter()
    for tokens in sentence_tokens:
        unique_tokens = set(tokens)
        for t in unique_tokens:
            doc_freq[t] += 1

    # 3. Compute TF-IDF
    vectors = []
    for tokens in sentence_tokens:
        term_freq = Counter(tokens)
        vector = {}
        for t, count in term_freq.items():
            tf = count
            idf = math.log(n_docs / (1 + doc_freq[t]))
            vector[t] = tf * idf
        vectors.append(vector)
    
    return vectors

def cosine_similarity(v1, v2):
    """Computes cosine similarity between two tf-idf vectors."""
    intersection = set(v1.keys()) & set(v2.keys())
    numerator = sum(v1[t] * v2[t] for t in intersection)
    
    sum1 = sum(val**2 for val in v1.values())
    sum2 = sum(val**2 for val in v2.values())
    denominator = math.sqrt(sum1) * math.sqrt(sum2)
    
    if denominator == 0:
        return 0.0
    return numerator / denominator

def summarize_text(text, max_sentences=5):
    """
    Main summarization function using LexRank.
    """
    if not text:
        return []

    # New Step: Clean HTML
    text = clean_html(text)

    sentences = split_sentences(text)
    # Filter out very short boilerplate
    sentences = [s for s in sentences if len(s.split()) > 3]

    if len(sentences) <= max_sentences:
        return sentences

    # 1. Vectorize
    vectors = compute_tf_idf(sentences)

    # 2. Build Similarity Matrix (adj list for sparse, or full matrix)
    n = len(sentences)
    # Using adjacency dictionary for "matrix" to save space if sparse, 
    # though here it's dense logic. 
    # Let's use simple list of lists for small N.
    matrix = [[0.0] * n for _ in range(n)]
    degree = [0.0] * n
    threshold = 0.1

    for i in range(n):
        for j in range(i + 1, n):
            sim = cosine_similarity(vectors[i], vectors[j])
            if sim > threshold:
                matrix[i][j] = sim
                matrix[j][i] = sim
                degree[i] += 1
                degree[j] += 1
    
    # 3. Power Iteration (PageRank)
    for i in range(n):
        for j in range(n):
            if degree[i] > 0:
                matrix[i][j] /= degree[i]
            else:
                matrix[i][j] = 1.0 / n
    
    scores = [1.0 / n] * n
    damping = 0.85
    iterations = 20

    for _ in range(iterations):
        new_scores = [0.0] * n
        for i in range(n):
            for j in range(n):
                new_scores[i] += scores[j] * matrix[j][i]
            new_scores[i] = (1 - damping) / n + damping * new_scores[i]
        scores = new_scores

    # 4. Rank and Select
    ranked_sentences = []
    for i in range(n):
        ranked_sentences.append((scores[i], i, sentences[i]))
    
    # Sort by score descending
    ranked_sentences.sort(key=lambda x: x[0], reverse=True)
    
    # Take top N
    top_n = ranked_sentences[:max_sentences]
    
    # Re-sort by original index to preserve flow
    top_n.sort(key=lambda x: x[1])
    
    return [s for _, _, s in top_n]
