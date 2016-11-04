FROM python:3.5

RUN cd /opt && \
    git clone https://github.com/hatmatter/metawatcher_query_collector.git && \
    cd metawatcher_query_collector && \
    pip install -r requirements.txt

WORKDIR /opt/metawatcher_query_collector
CMD ["python", "runner.py"]