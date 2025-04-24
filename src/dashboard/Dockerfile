# src/dashboard/Dockerfile

# 1. Base image
FROM python:3.10-slim

# 2. Set working directory
WORKDIR /app

# 3. Copy & install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy application code
COPY . .

# 5. Expose Dash’s default port
EXPOSE 8050

# 6. Run the Dash app on container start
CMD ["python", "app.py"]