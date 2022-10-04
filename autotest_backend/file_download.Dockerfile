FROM alpine:3.16.2

RUN apk add --no-cache curl

RUN mkdir -p /files

CMD curl -H "Authorization: ${AUTH_TYPE} ${CREDENTIALS}" "${URL}" --output /tmp/tmp.zip && unzip /tmp/tmp.zip -d /files
