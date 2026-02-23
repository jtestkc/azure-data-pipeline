# Databricks Notebook: RAG Pipeline
# Section 9.2: RAG Pipeline
# Vector search, chunking/embedding, LLM integration, evaluation

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain_community.embeddings import DatabricksEmbeddings
from langchain.schema import Document
from langchain.prompts import PromptTemplate
import mlflow
import json

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"
DOCUMENTS_SCHEMA = "documents"
VECTOR_INDEX_NAME = "sales_documents_index"
ENDPOINT_NAME = "llama-3-8b-sales-agent"

MLFLOW_EXPERIMENT = "/Shared/sales-analytics/rag-pipeline"

# ============================================================================
# SETUP
# ============================================================================
mlflow.set_experiment(MLFLOW_EXPERIMENT)

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{DOCUMENTS_SCHEMA}")

# ============================================================================
# SECTION 9.2.1: DOCUMENT INGESTION & CHUNKING
# ============================================================================
print("=== SECTION 9.2.1: DOCUMENT CHUNKING ===\n")

sample_documents = [
    {
        "title": "Q4 Sales Report",
        "content": """
        Q4 2024 Sales Performance Summary:
        
        Total Revenue: $15.2M (up 18% YoY)
        North America: $6.5M (+12%)
        Europe: $4.8M (+22%)
        Asia Pacific: $3.9M (+25%)
        
        Top Products:
        1. Enterprise Suite - $4.2M (35% of revenue)
        2. Standard License - $3.8M (31%)
        3. Professional Services - $2.5M (21%)
        
        Customer Growth: 2,450 new customers (+15%)
        Average Deal Size: $12,500 (+8%)
        """
    },
    {
        "title": "Product Pricing Guide",
        "content": """
        Sales Pricing Guide 2024:
        
        Enterprise Suite: $2,500/user/year
        Standard License: $800/user/year  
        Professional Services: $150/hour
        
        Volume Discounts:
        50-99 users: 10% off
        100-499 users: 20% off
        500+ users: 30% off
        
        Enterprise Agreement: Custom pricing, includes 24/7 support
        """
    },
    {
        "title": "Customer Support Playbook",
        "content": """
        Customer Escalation Procedures:
        
        Level 1: Account Manager handles
        Level 2: Regional Director involvement
        Level 3: Executive sponsor for accounts >$100K ARR
        
        SLA Response Times:
        Critical: 2 hours
        High: 4 hours
        Medium: 24 hours
        Low: 72 hours
        """
    }
]

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200,
    separators=["\n\n", "\n", " ", ""]
)

chunks = []
for doc in sample_documents:
    doc_chunks = text_splitter.split_text(doc["content"])
    for i, chunk in enumerate(doc_chunks):
        chunks.append({
            "title": doc["title"],
            "content": chunk,
            "chunk_id": f"{doc['title'].replace(' ', '_')}_{i}"
        })

print(f"Created {len(chunks)} chunks from {len(sample_documents)} documents")
for chunk in chunks[:3]:
    print(f"\n--- {chunk['title']} (chunk {chunk['chunk_id']}) ---")
    print(chunk["content"][:200] + "...")

# ============================================================================
# SECTION 9.2.2: EMBEDDING & VECTOR SEARCH
# ============================================================================
print("\n=== SECTION 9.2.2: EMBEDDING & VECTOR SEARCH ===\n")

chunks_df = spark.createDataFrame(chunks)

chunks_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG_NAME}.{DOCUMENTS_SCHEMA}.document_chunks")

print(f"Saved {chunks_df.count()} chunks to Delta table")

# Create Databricks vector search index
embedding_model = DatabricksEmbeddings(
    endpoint="databricks-bge-large-en"
)

# Create vector search index (uncomment in production)
# from databricks.vector_search import create_index
# 
# index = create_index(
#     index_name=VECTOR_INDEX_NAME,
#     source_table=f"{CATALOG_NAME}.{DOCUMENTS_SCHEMA}.document_chunks",
#     embedding_endpoint="databricks-bge-large-en",
#     text_column="content",
#     metadata_columns=["title", "chunk_id"]
# )

print(f"Vector search index: {VECTOR_INDEX_NAME}")

# ============================================================================
# SECTION 9.2.3: RAG PIPELINE
# ============================================================================
print("\n=== SECTION 9.2.3: RAG PIPELINE ===\n")

def retrieve_relevant_docs(query: str, k: int = 3):
    """Retrieve top-k relevant documents using vector search"""
    
    # Query embedding
    query_embedding = embedding_model.embed_query(query)
    
    # Search vector index
    results = spark.sql(f"""
        SELECT title, content, 
               cosine_similarity(embedding, array({','.join(map(str, query_embedding))})) as similarity
        FROM {CATALOG_NAME}.{DOCUMENTS_SCHEMA}.document_chunks
        ORDER BY similarity DESC
        LIMIT {k}
    """)
    
    return results.collect()

def generate_rag_response(query: str, context_docs: list) -> str:
    """Generate response using retrieved context"""
    
    context = "\n\n".join([
        f"Document: {doc['title']}\n{doc['content']}"
        for doc in context_docs
    ])
    
    prompt = f"""Based on the following context, answer the question. 
If the context doesn't contain enough information, say so.

Context:
{context}

Question: {query}

Answer:"""
    
    # Invoke LLM (in production, use actual endpoint)
    # response = invoke_llama(prompt)
    response = f"[LLM Response would appear here based on context]"
    
    return response

# ============================================================================
# SECTION 9.2.4: EVALUATION
# ============================================================================
print("\n=== SECTION 9.2.4: RAG EVALUATION ===\n")

test_queries = [
    {
        "query": "What was the Q4 revenue for Europe?",
        "expected": "Europe: $4.8M (+22%)"
    },
    {
        "query": "What are the volume discounts for 100-499 users?",
        "expected": "20% off"
    },
    {
        "query": "What is the SLA for critical issues?",
        "expected": "2 hours"
    }
]

eval_results = []

for test in test_queries:
    retrieved = retrieve_relevant_docs(test["query"], k=2)
    response = generate_rag_response(test["query"], retrieved)
    
    # Simple relevance check
    relevant = any(
        test["expected"].lower() in doc["content"].lower() 
        for doc in retrieved
    )
    
    eval_results.append({
        "query": test["query"],
        "expected": test["expected"],
        "retrieved_docs": len(retrieved),
        "relevant": relevant,
        "response": response[:100]
    })
    
    print(f"Q: {test['query']}")
    print(f"  Retrieved: {len(retrieved)} docs, Relevant: {relevant}")
    print(f"  Response: {response[:100]}...\n")

# ============================================================================
# LOG EXPERIMENTS TO MLFLOW
# ============================================================================
print("\n=== MLFLOW TRACKING ===\n")

with mlflow.start_run(run_name="rag-evaluation"):
    mlflow.log_params({
        "embedding_model": "databricks-bge-large-en",
        "chunk_size": 1000,
        "chunk_overlap": 200,
        "top_k": 3,
        "num_test_queries": len(test_queries)
    })
    
    relevant_count = sum(1 for r in eval_results if r["relevant"])
    mlflow.log_metrics({
        "retrieval_relevance": relevant_count / len(eval_results),
        "avg_retrieved_docs": sum(r["retrieved_docs"] for r in eval_results) / len(eval_results)
    })

print("RAG evaluation complete!")
print(f"Retrieval relevance: {relevant_count / len(eval_results) * 100:.1f}%")
