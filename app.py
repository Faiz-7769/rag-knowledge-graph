import chainlit as cl
from src.knowledge_graph.pipeline.rag_pipeline import RAGPipeline

# --- 1. Startup: Runs once when user opens the page ---
@cl.on_chat_start
async def start():
    """
    Initializes the RAG pipeline and sends a welcome message.
    """
    # A. Send a Loading Message
    msg = cl.Message(content="🚀 Initializing RAG Knowledge Graph System...")
    await msg.send()

    try:
        # B. Load the Chain (Static Method)
        chain = RAGPipeline.get_rag_chain()
        cl.user_session.set("chain", chain)
        
        # C. Update to Welcome Message
        msg.content = """
        # 👋 Welcome to Your Knowledge Graph AI!
        I am connected to your Neo4j Graph and Vector Database. 
        """
        await msg.update()

    except Exception as e:
        msg.content = f"❌ Error initializing system: {str(e)}"
        await msg.update()

# --- 2. Chat Loop: Runs every time user types ---
@cl.on_message
async def main(message: cl.Message):
    """
    Receives user message, runs RAG chain, and sends answer.
    """
    # Retrieve the chain from session
    chain = cl.user_session.get("chain")

    if not chain:
        await cl.Message(content="⚠️ Session expired. Please refresh the page.").send()
        return

    # Create an empty message to stream/update later
    msg = cl.Message(content="")
    await msg.send()

    try:
        # Run the Chain
        # We wrap it in make_async because LangChain's invoke is synchronous
        answer = await cl.make_async(chain.invoke)(message.content)
        
        # Send the Answer
        msg.content = answer
        await msg.update()

    except Exception as e:
        msg.content = f"⚠️ An error occurred: {str(e)}"
        await msg.update()