FROM python:3.9


WORKDIR /app


COPY --chown=user:user requirements.txt  .
RUN apt update && apt install -y cmake 
RUN adduser --disabled-password user
USER user
RUN python -m pip install -r requirements.txt
COPY --chown=user:user *.py .

CMD [ "python", "main.py" ] 

ARG GIT_COMMIT_SHA None
ENV GIT_COMMIT_SHA ${GIT_COMMIT_SHA}
