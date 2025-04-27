from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders.csv_loader import CSVLoader
from langchain_chroma import Chroma

load_dotenv()



embeddings = OpenAIEmbeddings(model="text-embedding-3-large")

loader = CSVLoader(
    "/Users/sbk/pseudo_lab/Lang2SQL/agent/pinecone/table_ddl.csv"
)

docs = loader.load()

index_name = "ddl"  

vector_store = Chroma(
    collection_name=index_name,
    embedding_function=embeddings,
    persist_directory="/Users/sbk/pseudo_lab/Lang2SQL/agent/vector_db",  # Where to save data locally, remove if not necessary
)

vector_store.add_documents(documents=docs)
