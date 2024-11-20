FROM dev.exactspace.co/python3.8-base-es2:r1
COPY *.py /src/
COPY . /src/
COPY main /src/
RUN chmod +x /src/*
RUN pip install schedule
WORKDIR /src
ENTRYPOINT ["./main"]
