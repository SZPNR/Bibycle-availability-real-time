FROM python:3
#ADD producer.py /

COPY . /

RUN pip install kafka-python
RUN pip install requests

#ENTRYPOINT [ "./run.py" ]
CMD ["python", "/python-producer/producer.py"]
