FROM pypy:2

RUN pip install six bitstring
COPY pybufrkit /opt/app/pybufrkit
ENV PYTHONPATH=/opt/app

ENTRYPOINT ["pypy", "-m", "pybufrkit"]

