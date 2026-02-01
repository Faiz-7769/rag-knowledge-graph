FROM python:3.10-slim

WORKDIR /code

# 1. Install requirements
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN python -m spacy download en_core_web_sm

# 2. Copy code and fix permissions
COPY . .
RUN mkdir -p /.chainlit && chmod 777 /.chainlit

# 3. CRITICAL: Use the exact host and port settings
CMD ["chainlit", "run", "app.py", "--host", "0.0.0.0", "--port", "7860"]
