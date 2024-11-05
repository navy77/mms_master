FROM python:3.9-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y gnupg2 curl
RUN apt-get install -y cron \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN apt-get update \
  && apt-get -y install gcc \
  && apt-get -y install g++ \
  && apt-get -y install unixodbc unixodbc-dev \
  && apt-get clean 
# Edit OpenSSL configuration to enable TLS 1.0
RUN sed -i 's/^\(MinProtocol =\).*/\1 TLSv1.0/' /etc/ssl/openssl.cnf

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN apt-get update && \
    apt-get install -y docker.io curl && \
    rm -rf /var/lib/apt/lists/*
    
COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
RUN apt install nano
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]