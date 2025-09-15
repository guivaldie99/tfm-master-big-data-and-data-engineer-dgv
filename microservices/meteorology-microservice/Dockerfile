FROM public.ecr.aws/docker/library/python:3.11-slim

RUN groupadd meteorology && useradd meteorology -d /meteorology -g meteorology
COPY src /meteorology
RUN chown -R meteorology:meteorology /meteorology
WORKDIR /meteorology

RUN pip install --no-cache-dir -r requirements.txt;

CMD ["python", "meteorology.py"]
