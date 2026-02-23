# Databricks Notebook: Fine-tuning Notebook
# Section 9.5: Fine-tuning with Mosaic AI Training
# Synthetic tickets, training, MLflow tracking, Unity Catalog

from faker import Faker
import pandas as pd
import mlflow
import json

# ============================================================================
# CONFIGURATION
# ============================================================================
CATALOG_NAME = "sales_catalog"
MLFLOW_EXPERIMENT = "/Shared/sales-analytics/finetuning"
FINE_TUNED_MODEL_NAME = "sales-support-ticket-classifier"

# ============================================================================
# SETUP
# ============================================================================
mlflow.set_experiment(MLFLOW_EXPERIMENT)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.ml")

fake = Faker()
Faker.seed(42)

# ============================================================================
# SECTION 9.5.1: GENERATE SYNTHETIC SUPPORT TICKETS
# ============================================================================
print("=== SECTION 9.5.1: GENERATE SYNTHETIC SUPPORT TICKETS ===\n")

ticket_categories = [
    "Billing Issue", "Technical Problem", "Account Access", 
    "Product Question", "Feature Request", "Complaint", "Refund Request"
]

priority_levels = ["Low", "Medium", "High", "Critical"]
sentiments = ["Positive", "Neutral", "Negative", "Frustrated", "Satisfied"]

def generate_ticket():
    category = fake.random_element(ticket_categories)
    priority = fake.random_element(priority_levels)
    sentiment = fake.random_element(sentiments)
    
    templates = {
        "Billing Issue": [
            "I was charged twice for my subscription. Please refund the duplicate charge.",
            "Can you explain the billing cycle? I was billed on the 1st and again on the 15th.",
            "My invoice shows incorrect tax amount. Please correct and reissue."
        ],
        "Technical Problem": [
            "The application crashes when I try to export data. Error code: ERR_500.",
            "I cannot login to my account. It keeps saying 'Invalid credentials'.",
            "The API is returning 503 errors intermittently."
        ],
        "Account Access": [
            "I need to reset my password. I didn't receive the reset email.",
            "Please add a new user to our team account.",
            "How do I enable 2FA for my account?"
        ],
        "Product Question": [
            "Does the Enterprise plan include 24/7 support?",
            "What is the difference between the Standard and Professional licenses?",
            "Can I upgrade my plan mid-cycle?"
        ],
        "Feature Request": [
            "It would be great to have dark mode in the dashboard.",
            "Can you add export to PDF functionality?",
            "Please add integrations with Slack and Teams."
        ],
        "Complaint": [
            "The service quality has degraded significantly. Response times are too slow.",
            "I've been waiting 3 days for a response to my support ticket.",
            "The new update is very confusing and hard to use."
        ],
        "Refund Request": [
            "I would like a full refund as the product doesn't meet my needs.",
            "The feature I signed up for is not available. Please cancel and refund.",
            "I was charged for a full year but only need the service for 3 months."
        ]
    }
    
    subject_templates = templates.get(category, ["General inquiry"])
    subject = fake.random_element(subject_templates)
    
    # Generate conversation history
    num_messages = fake.random_int(min=1, max=4)
    conversation = []
    for i in range(num_messages):
        is_customer = i % 2 == 0
        if is_customer:
            conversation.append({
                "role": "customer",
                "text": subject if i == 0 else fake.sentence()
            })
        else:
            conversation.append({
                "role": "agent",
                "text": fake.sentence() + " " + fake.sentence()
            })
    
    return {
        "ticket_id": f"TKT-{fake.random_number(digits=6)}",
        "category": category,
        "priority": priority,
        "sentiment": sentiment,
        "conversation": json.dumps(conversation),
        "subject": subject,
        "created_at": fake.date_time_this_year().isoformat()
    }

# Generate 1000 synthetic tickets
tickets = [generate_ticket() for _ in range(1000)]
tickets_df = pd.DataFrame(tickets)

print(f"Generated {len(tickets_df)} synthetic support tickets")
print(f"\nCategory Distribution:")
print(tickets_df['category'].value_counts())
print(f"\nPriority Distribution:")
print(tickets_df['priority'].value_counts())

# Save to Delta table
tickets_spark = spark.createDataFrame(tickets_df)
tickets_spark.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_NAME}.ml.support_tickets")

print(f"\nSaved to: {CATALOG_NAME}.ml.support_tickets")

# ============================================================================
# SECTION 9.5.2: PREPARE TRAINING DATA
# ============================================================================
print("\n=== SECTION 9.5.2: PREPARE TRAINING DATA ===\n")

# Create training dataset with prompt-response format
def create_training_example(row):
    conversation = json.loads(row['conversation'])
    chat_history = "\n".join([f"{msg['role']}: {msg['text']}" for msg in conversation])
    
    return {
        "prompt": f"""Ticket Category: {row['category']}
Priority: {row['priority']}
Sentiment: {row['sentiment']}

Conversation:
{chat_history}

Classify this ticket and provide recommended response:""",
        "response": f"Category: {row['category']}\nPriority: {row['priority']}\nRecommended Action: Handle {row['category'].lower()} with {row['priority'].lower()} priority. Customer sentiment: {row['sentiment']}."
    }

training_data = [create_training_example(row) for _, row in tickets_df.iterrows()]
training_df = pd.DataFrame(training_data)

print(f"Created {len(training_df)} training examples")
print("\nSample training example:")
print(training_df.iloc[0]['prompt'][:200] + "...")
print("\nResponse:")
print(training_df.iloc[0]['response'])

# Split into train/validation
train_size = int(len(training_df) * 0.8)
train_df = training_df[:train_size]
val_df = training_df[train_size:]

print(f"\nTrain size: {len(train_df)}")
print(f"Validation size: {len(val_df)}")

# ============================================================================
# SECTION 9.5.3: MOSAIC AI TRAINING
# ============================================================================
print("\n=== SECTION 9.5.3: MOSAIC AI TRAINING ===\n")

# Note: This is the configuration for Mosaic AI Training
# Actual training would be done via the Mosaic AI Training API

training_config = {
    "task_type": "instruction_finetuning",
    "base_model": "meta-llama-3-8b",
    "dataset": {
        "train_path": f"{CATALOG_NAME}.ml.support_tickets",
        "format": "instruction"
    },
    "hyperparameters": {
        "learning_rate": 2e-5,
        "num_epochs": 3,
        "batch_size": 8,
        "max_seq_length": 2048,
        "warmup_steps": 100
    },
    "compute": {
        "gpu_type": "A100",
        "num_gpus": 2
    },
    "output": {
        "model_name": FINE_TUNED_MODEL_NAME,
        "catalog": CATALOG_NAME,
        "schema": "ml"
    }
}

print("Training Configuration:")
print(json.dumps(training_config, indent=2))

# ============================================================================
# SECTION 9.5.4: MLFLOW TRACKING
# ============================================================================
print("\n=== SECTION 9.5.4: MLFLOW TRACKING ===\n")

with mlflow.start_run(run_name="finetuning-support-ticket-classifier"):
    # Log training config
    mlflow.log_params({
        "base_model": training_config["base_model"],
        "task_type": training_config["task_type"],
        "learning_rate": training_config["hyperparameters"]["learning_rate"],
        "num_epochs": training_config["hyperparameters"]["num_epochs"],
        "batch_size": training_config["hyperparameters"]["batch_size"],
        "num_gpus": training_config["compute"]["num_gpus"],
        "train_samples": len(train_df),
        "val_samples": len(val_df)
    })
    
    # Log metrics (simulated)
    mlflow.log_metrics({
        "train_loss": 0.45,
        "val_loss": 0.38,
        "train_accuracy": 0.89,
        "val_accuracy": 0.85,
        "epoch": 3
    })

print("MLflow tracking complete!")

# ============================================================================
# SECTION 9.5.5: REGISTER MODEL TO UNITY CATALOG
# ============================================================================
print("\n=== SECTION 9.5.5: UNITY CATALOG REGISTRATION ===\n")

# Register the fine-tuned model
model_uri = f"runs:/latest/{FINE_TUNED_MODEL_NAME}"

registered_model = mlflow.register_model(
    model_uri,
    f"{CATALOG_NAME}.ml.{FINE_TUNED_MODEL_NAME}"
)

print(f"Registered model: {registered_model.name}")
print(f"Model version: {registered_model.version}")

# Add model description
client = mlflow.tracking.MlflowClient()
client.update_model_version(
    name=f"{CATALOG_NAME}.ml.{FINE_TUNED_MODEL_NAME}",
    version=registered_model.version,
    description="Fine-tuned Llama 3 for sales support ticket classification"
)

# Add tags
client.set_model_version_tag(
    name=f"{CATALOG_NAME}.ml.{FINE_TUNED_MODEL_NAME}",
    version=registered_model.version,
    key="task_type",
    value="ticket_classification"
)

print("\nModel registered to Unity Catalog!")
print(f"Full name: {CATALOG_NAME}.ml.{FINE_TUNED_MODEL_NAME}")
