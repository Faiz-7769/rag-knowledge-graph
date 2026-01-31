import os
import time  # <--- 1. Import Time
import pandas as pd
from deepeval.metrics import (
    AnswerRelevancyMetric,
    FaithfulnessMetric,
    ContextualPrecisionMetric,
    ContextualRecallMetric
)
from deepeval.test_case import LLMTestCase
from deepeval.models.base_model import DeepEvalBaseLLM
from langchain_groq import ChatGroq
from dotenv import load_dotenv

from src.knowledge_graph.pipeline.rag_pipeline import RAGPipeline
from test_data import eval_data

load_dotenv()

# --- 1. Custom Model Wrapper ---
class GroqModel(DeepEvalBaseLLM):
    def __init__(self, model):
        self.model = model

    def load_model(self):
        return self.model

    def generate(self, prompt: str) -> str:
        return self.model.invoke(prompt).content

    async def a_generate(self, prompt: str) -> str:
        return self.generate(prompt)

    def get_model_name(self):
        return "Groq Llama 3.3"

# --- 2. Setup ---
groq_judge = ChatGroq(
    model="llama-3.3-70b-versatile",
    api_key=os.getenv("GROQ_API_KEY"),
    temperature=0
)
groq_evaluator = GroqModel(model=groq_judge)

print("Initializing RAG Pipeline...")
# Call Static Method directly
chain = RAGPipeline.get_rag_chain()

# Initialize Metrics
metrics = [
    AnswerRelevancyMetric(threshold=0.5, model=groq_evaluator),
    FaithfulnessMetric(threshold=0.5, model=groq_evaluator),
    ContextualPrecisionMetric(threshold=0.5, model=groq_evaluator),
    ContextualRecallMetric(threshold=0.5, model=groq_evaluator)
]

print("="*80)
print("DEEPEVAL RAG EVALUATION (With Rate Limiting)")
print("="*80)

results = []

# --- 3. Evaluation Loop ---
for i, item in enumerate(eval_data, 1):
    try:
        print(f"\n[{i}/{len(eval_data)}] Evaluating: {item['question'][:60]}...")
        
        # A. RAG Generation
        response = chain.invoke(item["question"])
        
        # B. Context Retrieval
        retriever_out = chain.steps[0].invoke(item["question"])
        raw_docs = retriever_out["context"] 
        contexts = [doc.page_content for doc in raw_docs]

        # C. Create Test Case
        test_case = LLMTestCase(
            input=item["question"],
            actual_output=response,
            expected_output=item["ground_truth"],
            retrieval_context=contexts
        )

        row_result = {
            "question": item["question"],
            "answer": response,
            "expected": item["ground_truth"]
        }
        
        # D. Measure Metrics (With Delays)
        for metric in metrics:
            metric.measure(test_case)
            print(f"  {metric.__class__.__name__}: {metric.score:.2f} {'✓' if metric.is_successful() else '✗'}")
            
            # Save score
            name = metric.__class__.__name__
            row_result[name] = metric.score
            row_result[f"{name}_pass"] = metric.is_successful()
            
            # 2. Small Delay between each metric check (Prevents burst limit)
            time.sleep(1) 

        results.append(row_result)
        
        # 3. Major Delay between questions (Refills your RPM bucket)
        print("Cooling down for 3 seconds...")
        time.sleep(3)

    except Exception as e:
        print(f"Error evaluating question {i}: {e}")
        time.sleep(5) # Wait longer if we hit an error

# --- 4. Save & Summary ---
if results:
    df = pd.DataFrame(results)
    df.to_csv("deepeval_results.csv", index=False)
    print("\nResults saved to: deepeval_results.csv")
else:
    print("No results generated.")