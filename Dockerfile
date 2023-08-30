FROM prefecthq/prefect:2.11.4-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org