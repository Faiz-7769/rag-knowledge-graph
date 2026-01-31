import streamlit as st
from src.knowledge_graph.pipeline.rag_pipeline import RAGPipeline

# --- Page Configuration ---
st.set_page_config(
    page_title="ragvec",
    page_icon="🤖",
    layout="wide"
)

# Set the header/title
st.title("🤖 ragvec")
st.caption("Your Knowledge Graph AI connected to Neo4j & Vector Database")

# --- 1. Startup & Initialization ---

# Cache the chain loader so it only runs once per session (not on every message)
@st.cache_resource(show_spinner=False)
def load_rag_chain():
    """Load the RAG chain and cache it."""
    return RAGPipeline.get_rag_chain()

# Initialize Chat History if it doesn't exist
if "messages" not in st.session_state:
    st.session_state["messages"] = [
        {"role": "assistant", "content": "# 👋 Welcome to Your Knowledge Graph AI!\nI am connected to your Neo4j Graph and Vector Database."}
    ]

# Load the Chain (Display spinner only on first load)
if "chain" not in st.session_state:
    with st.spinner("🚀 Initializing RAG Knowledge Graph System..."):
        try:
            st.session_state["chain"] = load_rag_chain()
        except Exception as e:
            st.error(f"❌ Error initializing system: {str(e)}")

# --- 2. Chat Loop ---

# Display all previous messages in the chat history
for msg in st.session_state["messages"]:
    # We use the name "ragvec" for the assistant messages
    role_name = "ragvec" if msg["role"] == "assistant" else "user"
    avatar = "🤖" if msg["role"] == "assistant" else None
    
    with st.chat_message(msg["role"], avatar=avatar):
        st.markdown(msg["content"])

# Handle new user input
if prompt := st.chat_input("Ask ragvec a question..."):
    
    # 1. Display User Message Immediately
    st.session_state["messages"].append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 2. Generate and Display Assistant Response
    with st.chat_message("assistant", avatar="🤖"):
        message_placeholder = st.empty()
        message_placeholder.markdown("Thinking...")

        try:
            chain = st.session_state.get("chain")
            if chain:
                # Run the chain
                # Note: Streamlit is sync by default, so we just invoke directly
                response = chain.invoke(prompt)
                
                # Display final answer
                message_placeholder.markdown(response)
                
                # Save to history
                st.session_state["messages"].append({"role": "assistant", "content": response})
            else:
                message_placeholder.error("⚠️ Session expired. Please refresh.")
        except Exception as e:
            error_msg = f"⚠️ An error occurred: {str(e)}"
            message_placeholder.error(error_msg)
            st.session_state["messages"].append({"role": "assistant", "content": error_msg})