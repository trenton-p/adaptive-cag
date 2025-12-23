import os
import boto3

from typing import Literal, TypedDict, List, Generator, Any
from botocore.config import Config
from langchain_aws import ChatBedrock
from langchain_core.output_parsers import StrOutputParser
from langgraph.graph import StateGraph, START, END
from langgraph.graph.state import CompiledStateGraph
from langchain_core.prompts import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate
)
from utils import (
    get_context,
    get_namespace
)

REGION = os.environ["AWS_REGION"]
TEXT_MODEL = os.environ["BEDROCK_TEXT_MODEL"]
MODEL = ChatBedrock(
    model_id=TEXT_MODEL,
    client=boto3.client(
        "bedrock-runtime",
        region_name=REGION,
        config=Config(connect_timeout=5, read_timeout=60, retries={"total_max_attempts": 20, "mode": "adaptive"})
    ),
    model_kwargs={"temperature": 0},
    # NOTE: Test streaming on the LLM
    streaming=True
)

# Agent graph state
class GraphState(TypedDict):
    """
    Represents the state of the graph.

    Attributes:
        question: question
        generation: LLM generation
        context: list of retrieved context chunks
    """

    question: str
    answer: str
    context: List[str]


# Agent graph nodes
def generate_answer(state):
    print("---GENERATE ANSWER---")
    question = state["question"]
    context = state["context"]

    # Create the system prompt for the LLM call
    system_message = """
    You are a helpful news article search assistant. Your task is to provide an accurate, and relevant answer to a user's question.
    Use the provided news articles, provided as context to answer the user's question. If there is not supporting context to properly answer the question, 
    politely indicate that you don't have any supporting news information to properly answer the question.
    """
    
    # Create the question prompt/context for the LLM call
    human_message = """
    The context is provided as: {context}
    Th question is provided as: {question}
    """

    # Compile the prompt
    prompt = ChatPromptTemplate(
        messages=[
            SystemMessagePromptTemplate.from_template(system_message),
            HumanMessagePromptTemplate.from_template(human_message)
        ],
        input_variables=["context", "question"]
    )

    # Define the runnable for the state
    runnable = prompt | MODEL | StrOutputParser()

    # Invoke the state runnable to answer the question
    answer = runnable.invoke({"context": context, "question": question})

    return {"question": question, "answer": answer, "context": context}


def tech_retriever(state):
    print("---TECH CONTEXT RETRIEVAL---")
    question = state["question"]

    # Get the contextual text for the question, using the `tech` namespace
    context = get_context(text=question, namespace="tech")

    # Return the updated `state`
    return {"question": question, "context": context}


def world_retriever(state):
    print("---WORLD CONTEXT RETRIEVAL---")
    question = state["question"]

    # Get the contextual text for the question, using the `world` namespace
    context = get_context(text=question, namespace="world")

    # Return the updated `state`
    return {"question": question, "context": context}


def sports_retriever(state):
    print("---SPORTS CONTEXT RETRIEVAL---")
    question = state["question"]

    # Get the contextual text for the question, using the `sports` namespace
    context = get_context(text=question, namespace="sports")

    # Return the `state`
    return {"question": question, "context": context}


def business_retriever(state):
    print("---BUSINESS CONTEXT RETRIEVAL---")
    question = state["question"]

    # Get the contextual text for the question, using the `business` namespace
    context = get_context(text=question, namespace="business")

    # Return the `state`
    return {"question": question, "context": context}


# Agent graph actions
def route_question(state) -> Literal["tech", "world", "sports", "business"]:
    print("---ROUTING QUESTION---")
    question = state["question"]

    # Classify the question for semantic similarity, against the `router` namespace.
    # NOTE: This determines which contextual retriever to use.
    namespace = get_namespace(text=question)

    return namespace


# Build agent workflow
def build_graph() -> CompiledStateGraph:
    # Define the workflow `state`
    workflow = StateGraph(GraphState)

    # Define the graph node for the `tech` namespace retriever
    workflow.add_node("tech_retriever", tech_retriever)

    # Define the graph node for the `world` namespace retriever
    workflow.add_node("world_retriever", world_retriever)

    # Define the graph node for the `sports` namespace retriever
    workflow.add_node("sports_retriever", sports_retriever)

    # Define the graph node for the `business` namespace retriever
    workflow.add_node("business_retriever", business_retriever)

    # Define the graph node to generate a response using the retrieved context
    workflow.add_node("generate_answer", generate_answer)

    # Connect the nodes to establish the graph flow
    workflow.add_conditional_edges(
        START,
        route_question,
        {
            "tech": "tech_retriever",
            "world": "world_retriever",
            "sports": "sports_retriever",
            "business": "business_retriever"
        }
    )
    workflow.add_edge("tech_retriever", "generate_answer")
    workflow.add_edge("world_retriever", "generate_answer")
    workflow.add_edge("sports_retriever", "generate_answer")
    workflow.add_edge("business_retriever", "generate_answer")
    workflow.add_edge("generate_answer", END)

    # Compile the graph
    graph = workflow.compile()

    return graph


# Run agent workflow
def run_agent(question: str) -> Generator[str, Any, None]:
    graph = build_graph()
    for output in graph.stream(input={"question": question}, stream_mode="values"):
        if "answer" in output:
            yield output["answer"]