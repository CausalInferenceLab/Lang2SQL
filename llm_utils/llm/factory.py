import os
from typing import Optional

from langchain.llms.base import BaseLanguageModel
from langchain_aws import ChatBedrockConverse, BedrockEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_huggingface import (
    ChatHuggingFace,
    HuggingFaceEndpoint,
    HuggingFaceEndpointEmbeddings,
)
from langchain_ollama import ChatOllama, OllamaEmbeddings
from langchain_openai import (
    AzureOpenAIEmbeddings,
    ChatOpenAI,
    AzureChatOpenAI,
    OpenAIEmbeddings,
)


def get_llm(**kwargs) -> BaseLanguageModel:
    """
    return chat model interface
    """
    provider = os.getenv("LLM_PROVIDER")
    print(os.environ["LLM_PROVIDER"])

    if provider is None:
        raise ValueError("LLM_PROVIDER environment variable is not set.")

    if provider == "openai":
        return get_llm_openai(**kwargs)

    elif provider == "azure":
        return get_llm_azure(**kwargs)

    elif provider == "bedrock":
        return get_llm_bedrock(**kwargs)

    elif provider == "gemini":
        return get_llm_gemini(**kwargs)

    elif provider == "ollama":
        return get_llm_ollama(**kwargs)

    elif provider == "huggingface":
        return get_llm_huggingface(**kwargs)

    else:
        raise ValueError(f"Invalid LLM API Provider: {provider}")


def get_llm_openai(**kwargs) -> BaseLanguageModel:
    return ChatOpenAI(
        model=os.getenv("OPEN_AI_LLM_MODEL", "gpt-4o"),
        api_key=os.getenv("OPEN_AI_KEY"),
        **kwargs,
    )


def get_llm_azure(**kwargs) -> BaseLanguageModel:
    return AzureChatOpenAI(
        api_key=os.getenv("AZURE_OPENAI_LLM_KEY"),
        azure_endpoint=os.getenv("AZURE_OPENAI_LLM_ENDPOINT"),
        azure_deployment=os.getenv("AZURE_OPENAI_LLM_MODEL"),  # Deployment name
        api_version=os.getenv("AZURE_OPENAI_LLM_API_VERSION", "2023-07-01-preview"),
        **kwargs,
    )


def get_llm_bedrock(**kwargs) -> BaseLanguageModel:
    return ChatBedrockConverse(
        model=os.getenv("AWS_BEDROCK_LLM_MODEL"),
        aws_access_key_id=os.getenv("AWS_BEDROCK_LLM_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_BEDROCK_LLM_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_BEDROCK_LLM_REGION", "us-east-1"),
        **kwargs,
    )


def get_llm_gemini(**kwargs) -> BaseLanguageModel:
    return ChatGoogleGenerativeAI(model=os.getenv("GEMINI_LLM_MODEL"), **kwargs)


def get_llm_ollama(**kwargs) -> BaseLanguageModel:
    base_url = os.getenv("OLLAMA_LLM_BASE_URL")
    if base_url:
        return ChatOllama(
            base_url=base_url, model=os.getenv("OLLAMA_LLM_MODEL"), **kwargs
        )
    else:
        return ChatOllama(model=os.getenv("OLLAMA_LLM_MODEL"), **kwargs)


def get_llm_huggingface(**kwargs) -> BaseLanguageModel:
    return ChatHuggingFace(
        llm=HuggingFaceEndpoint(
            model=os.getenv("HUGGING_FACE_LLM_MODEL"),
            repo_id=os.getenv("HUGGING_FACE_LLM_REPO_ID"),
            task="text-generation",
            endpoint_url=os.getenv("HUGGING_FACE_LLM_ENDPOINT"),
            huggingfacehub_api_token=os.getenv("HUGGING_FACE_LLM_API_TOKEN"),
            **kwargs,
        )
    )


def get_embeddings() -> Optional[BaseLanguageModel]:
    """
    return embedding model interface
    """
    provider = os.getenv("EMBEDDING_PROVIDER")
    print(provider)

    if provider is None:
        raise ValueError("EMBEDDING_PROVIDER environment variable is not set.")

    if provider == "openai":
        return get_embeddings_openai()

    elif provider == "bedrock":
        return get_embeddings_bedrock()

    elif provider == "azure":
        return get_embeddings_azure()

    elif provider == "gemini":
        return get_embeddings_gemini()

    elif provider == "ollama":
        return get_embeddings_ollama()

    else:
        raise ValueError(f"Invalid Embedding API Provider: {provider}")


def get_embeddings_openai() -> BaseLanguageModel:
    return OpenAIEmbeddings(
        model=os.getenv("OPEN_AI_EMBEDDING_MODEL"),
        openai_api_key=os.getenv("OPEN_AI_KEY"),
    )


def get_embeddings_azure() -> BaseLanguageModel:
    return AzureOpenAIEmbeddings(
        api_key=os.getenv("AZURE_OPENAI_EMBEDDING_KEY"),
        azure_endpoint=os.getenv("AZURE_OPENAI_EMBEDDING_ENDPOINT"),
        azure_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_MODEL"),
        api_version=os.getenv("AZURE_OPENAI_EMBEDDING_API_VERSION"),
    )


def get_embeddings_bedrock() -> BaseLanguageModel:
    return BedrockEmbeddings(
        model_id=os.getenv("AWS_BEDROCK_EMBEDDING_MODEL"),
        aws_access_key_id=os.getenv("AWS_BEDROCK_EMBEDDING_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_BEDROCK_EMBEDDING_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_BEDROCK_EMBEDDING_REGION", "us-east-1"),
    )


def get_embeddings_gemini() -> BaseLanguageModel:
    return GoogleGenerativeAIEmbeddings(
        model=os.getenv("GEMINI_EMBEDDING_MODEL"),
        api_key=os.getenv("GEMINI_EMBEDDING_KEY"),
    )


def get_embeddings_ollama() -> BaseLanguageModel:
    return OllamaEmbeddings(
        model=os.getenv("OLLAMA_EMBEDDING_MODEL"),
        base_url=os.getenv("OLLAMA_EMBEDDING_BASE_URL"),
    )


def get_embeddings_huggingface() -> BaseLanguageModel:
    return HuggingFaceEndpointEmbeddings(
        model=os.getenv("HUGGING_FACE_EMBEDDING_MODEL"),
        repo_id=os.getenv("HUGGING_FACE_EMBEDDING_REPO_ID"),
        huggingfacehub_api_token=os.getenv("HUGGING_FACE_EMBEDDING_API_TOKEN"),
    )
