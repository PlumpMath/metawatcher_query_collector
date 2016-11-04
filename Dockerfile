FROM python:3.5

CMD cd /opt && \
    git clone https://github.com/hatmatter/metawatcher_query_collector.git && \
    cd metawatcher_query_collector && \
    pip install -r requirements.txt && \
    python -u runner.py