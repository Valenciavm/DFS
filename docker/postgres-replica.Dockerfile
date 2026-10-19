FROM postgres:16-alpine

COPY scripts/start-postgres-replica.sh /usr/local/bin/start-postgres-replica
RUN chmod 0555 /usr/local/bin/start-postgres-replica

USER postgres
ENTRYPOINT ["/usr/local/bin/start-postgres-replica"]
