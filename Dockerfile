FROM python:3.10-slim

WORKDIR /code

# 1. Install CPU-only PyTorch first (Makes build fast!)
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu

# 2. Install the rest of the requirements
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 3. Download Spacy Model
RUN python -m spacy download en_core_web_sm

# 4. Copy app code
COPY . .

# 5. Fix permissions
RUN mkdir -p /.chainlit && chmod 777 /.chainlit

# 6. Run the app
CMD ["chainlit", "run", "app.py", "--host", "0.0.0.0", "--port", "7860"]