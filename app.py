import chainlit as cl
import asyncio
from src.knowledge_graph.pipeline.rag_pipeline import RAGPipeline

# --- 1. Startup ---
@cl.on_chat_start
async def start():
    msg = cl.Message(content="🚀 Initializing RAG Knowledge Graph System...")
    await msg.send()

    try:
        # Load the chain
        chain = RAGPipeline.get_rag_chain()
        cl.user_session.set("chain", chain)
        
        msg.content = """
        # 👋 Welcome to Your Knowledge Graph AI!
        I am connected to your Neo4j Graph and Vector Database.
        """
        await msg.update()

    except Exception as e:
        msg.content = f"❌ Error initializing system: {str(e)}"
        await msg.update()

# --- 2. Chat Loop ---
@cl.on_message
async def main(message: cl.Message):
    chain = cl.user_session.get("chain")

    if not chain:
        await cl.Message(content="⚠️ Session expired. Please refresh.").send()
        return

    # --- 🟢 VISUAL FEATURE: Dynamic "Thinking" Simulation ---
    # We create a manual step that stays open while we "load"
    async with cl.Step(name="Reasoning Engine", type="run") as step:
        
        try:
            # --- REAL CHAIN EXECUTION ---
            # We await the real answer now.
            # Note: We do NOT use callbacks here to keep the UI clean (just the box above).
            res = await chain.ainvoke(message.content)
            
            # --- DATA EXTRACTION ---
            answer_text = ""
            source_docs = []
            
            # Handle Dict Return (If you updated rag_pipeline.py)
            if isinstance(res, dict):
                answer_text = res.get("result") or res.get("output") or str(res)
                source_docs = res.get("source_documents", [])
            else:
                # Fallback if pipeline returns string
                answer_text = str(res)

            # Update the Step to show success
            step.output = "✅ Generation Complete"

        except Exception as e:
            step.output = f"❌ Error: {str(e)}"
            await cl.Message(content=f"⚠️ Internal Error: {str(e)}").send()
            return

    # --- 🔵 FINAL OUTPUT DISPLAY ---
    metric_display = ""
    
    if source_docs:
        metric_display = f"""
        \n\n---
        **📊 Confidence Score:** High 🟢
        **📚 Evidence:** Found {len(source_docs)} source(s)
        """
        # List Sources
        metric_display += "\n**References:**"
        for doc in source_docs[:3]:
            # Safe metadata access
            meta = getattr(doc, 'metadata', {})
            src = meta.get('source', 'Unknown File').split('/')[-1]
            page = meta.get('page', '1')
            metric_display += f"\n- `{src}` (Pg {page})"
            
    elif isinstance(res, str):
         # If pipeline returns only string, we assume generic confidence
         pass 
    else:
        metric_display = "\n\n---\n**📊 Confidence:** Low 🔴 (No database context found)"

    # Send final message
    await cl.Message(content=answer_text + metric_display).send()