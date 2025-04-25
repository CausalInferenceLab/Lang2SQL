from dotenv import load_dotenv
load_dotenv()

from langchain_pinecone import PineconeVectorStore
from langchain_openai import OpenAIEmbeddings


def pinecone_retriever_invoke(index_name, k):

    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
    vectorstore = PineconeVectorStore(index_name=index_name, embedding=embeddings)
    retriever = vectorstore.as_retriever(
        search_type="similarity", search_kwargs={"k": k}
    )
    return retriever