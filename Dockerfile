ARG BUILD_FROM
FROM $BUILD_FROM

# No Python dependencies - the broker is pure stdlib.
RUN apk add --no-cache python3

COPY broker.py /broker.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
