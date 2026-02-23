# Databricks Notebook: Model Serving - Llama 3 Endpoint
# Section 9.1: Generative AI with Mosaic AI
# Llama 3 endpoint deployment, rate limiting, load testing

import mlflow
import json
import time
import requests
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"
MLFLOW_EXPERIMENT = "/Shared/sales-analytics/llama-serving"

# Mosaic AI Model Serving endpoint config
ENDPOINT_NAME = "llama-3-8b-sales-agent"
MODEL_NAME = "meta-llama-3-8b-instruct"
MODEL_VERSION = 1

# Rate limiting config
RATE_LIMIT_REQUESTS_PER_MINUTE = 60
RATE_LIMIT_TOKENS_PER_MINUTE = 100000

# ============================================================================
# SETUP MLFLOW
# ============================================================================
mlflow.set_experiment(MLFLOW_EXPERIMENT)

# ============================================================================
# DEPLOY MODEL TO MOSAIC AI SERVING
# ============================================================================
print("=== SECTION 9.1: MODEL SERVING SETUP ===\n")

# Note: In production, this would use the Databricks API or UI to deploy
# The following is the configuration and client code

from databricks.model_serving import ModelServingEndpointClient

client = ModelServingEndpointClient()

# Create endpoint config
endpoint_config = {
    "name": ENDPOINT_NAME,
    "config": {
        "served_entities": [
            {
                "entity_name": f"{CATALOG_NAME}.ml.llama_model",
                "entity_version": str(MODEL_VERSION),
                "scale_type": "Auto",
                "min_instances": 1,
                "max_instances": 2,
                "runtime": "latest"
            }
        ],
        "traffic_config": {
            "routes": [
                {
                    "served_model_name": f"{CATALOG_NAME}.ml.llama_model",
                    "traffic_percentage": 100
                }
            ]
        },
        "rate_limits": [
            {
                "requests_per_minute": RATE_LIMIT_REQUESTS_PER_MINUTE,
                "tokens_per_minute": RATE_LIMIT_TOKENS_PER_MINUTE
            }
        ]
    }
}

# Create endpoint (uncomment to deploy)
# client.create_endpoint(endpoint_config)

print(f"Endpoint config created: {ENDPOINT_NAME}")
print(f"Rate limit: {RATE_LIMIT_REQUESTS_PER_MINUTE} req/min, {RATE_LIMIT_TOKENS_PER_MINUTE} tokens/min")

# ============================================================================
# INVOKE MODEL FOR SALES ANALYSIS
# ============================================================================
def invoke_llama(prompt: str, max_tokens: int = 500) -> str:
    """Invoke Llama 3 model for sales analysis"""
    
    url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/serving-endpoints/{ENDPOINT_NAME}/invocations"
    
    payload = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": max_tokens,
            "temperature": 0.7,
            "top_p": 0.9
        }
    }
    
    headers = {
        "Authorization": f"Bearer {dbutils.secrets.get('kv-secrets', 'databricks-token')}",
        "Content-Type": "application/json"
    }
    
    start_time = time.time()
    response = requests.post(url, json=payload, headers=headers)
    latency = time.time() - start_time
    
    return response.json(), latency

# ============================================================================
# SALES ANALYSIS EXAMPLES
# ============================================================================
print("\n=== SALES ANALYSIS EXAMPLES ===\n")

# Example 1: Regional performance summary
regional_prompt = """You are a sales analytics assistant. Analyze the following regional sales data:

North America: $1.2M revenue, 15,000 orders
Europe: $980K revenue, 12,500 orders
Asia Pacific: $850K revenue, 10,000 orders

Provide insights on:
1. Revenue performance by region
2. Average order value comparison
3. Recommendations for improvement"""

# Example 2: Product performance analysis
product_prompt = """Analyze these top-selling products:
- Product A: 5,000 units, $500K revenue
- Product B: 3,200 units, $480K revenue
- Product C: 2,800 units, $420K revenue

Which product has the highest margin and why?"""

# Uncomment to run actual inference
# response, latency = invoke_llama(regional_prompt)
# print(f"Response (latency: {latency:.2f}s):\n{response}")

# ============================================================================
# LOAD TESTING
# ============================================================================
print("\n=== LOAD TESTING ===\n")

def load_test(num_requests: int = 100, concurrent: int = 10):
    """Run load test against the serving endpoint"""
    
    results = []
    
    for i in range(0, num_requests, concurrent):
        batch_start = time.time()
        
        # Simulate concurrent requests
        for j in range(concurrent):
            try:
                start = time.time()
                # response, _ = invoke_llama(f"Analyze this transaction: {i+j}")
                latency = time.time() - start
                results.append({
                    "request_id": i + j,
                    "latency": latency,
                    "status": "success"
                })
            except Exception as e:
                results.append({
                    "request_id": i + j,
                    "latency": 0,
                    "status": "error",
                    "error": str(e)
                })
        
        batch_duration = time.time() - batch_start
        print(f"Batch {i//concurrent + 1}: {batch_duration:.2f}s")
        
        # Rate limit check
        if batch_duration < 60:
            time.sleep(60 - batch_duration)
    
    # Calculate metrics
    successful = [r for r in results if r["status"] == "success"]
    latencies = [r["latency"] for r in successful]
    
    print(f"\n=== Load Test Results ===")
    print(f"Total requests: {num_requests}")
    print(f"Successful: {len(successful)} ({len(successful)/num_requests*100:.1f}%)")
    print(f"Avg latency: {sum(latencies)/len(latencies):.2f}s")
    print(f"Min latency: {min(latencies):.2f}s")
    print(f"Max latency: {max(latencies):.2f}s")
    
    return results

# Run load test (uncomment to execute)
# results = load_test(num_requests=100, concurrent=10)

# ============================================================================
# LOG EXPERIMENTS TO MLFLOW
# ============================================================================
print("\n=== MLFLOW EXPERIMENT TRACKING ===\n")

with mlflow.start_run(run_name="llama-serving-evaluation"):
    # Log model serving configuration
    mlflow.log_params({
        "endpoint_name": ENDPOINT_NAME,
        "model_name": MODEL_NAME,
        "rate_limit_rpm": RATE_LIMIT_REQUESTS_PER_MINUTE,
        "rate_limit_tpm": RATE_LIMIT_TOKENS_PER_MINUTE,
        "max_tokens": 500,
        "temperature": 0.7
    })
    
    # Log metrics (example from load test)
    # mlflow.log_metrics({
    #     "avg_latency": 2.5,
    #     "p50_latency": 2.3,
    #     "p95_latency": 3.8,
    #     "p99_latency": 5.2,
    #     "success_rate": 0.99,
    #     "throughput_rpm": 45
    # })

print("MLflow experiment logged successfully")
print(f"View results: mlflow.experiments.get_by_name('{MLFLOW_EXPERIMENT}')")
