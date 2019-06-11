from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords, wordnet

from gensim.models import KeyedVectors

def pos_tag(pos):
    if pos.startswith('J'):
        return wordnet.ADJ
    elif pos.startswith('V'):
        return wordnet.VERB
    elif pos.startswith('N'):
        return wordnet.NOUN
    elif pos.startswith('R'):
        return wordnet.ADV
    else:          
        return None


def process(doc):
    # tockenize
    tokenized_doc = word_tokenize(doc)
    # remove stop wordq
    # lemmantize (better best good -> good)
    # with pos_tag
    pos_doc = nltk.pos_tag(tokenized_doc)
    stop_words=set(stopwords.words("english"))
    lem = WordNetLemmatizer()
    procesed_doc=[]
    for (word, pos) in pos_doc:
        if word not in stop_words:
            tag = pos_tag(pos)
            word = lem.lemmatize(word, tag)
            procesed_doc.append(word)
    return procesed_doc


def embedding(processed_doc):
    embedder = KeyedVectors.load_word2vec_format('wiki.en/wiki.en.vec')
    embedded_vector = [embedder[i] for i in procesed_doc]
    return embedded_vector

    # (Word# x 300)




# text="""Hello Mr. Smith, how are you doing today? The weather is great, and city is awesome.
# The sky is pinkish-blue. You shouldn't eat cardboard"""
# tokenized_text=sent_tokenize(text)
# print(tokenized_text)
