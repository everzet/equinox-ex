#!/bin/bash
set -e

# message-db configuration
export MDBVERSION=${MESSAGEDB_VERSION:-v1.3.0}
export MDBUSER=${MESSAGEDB_USER:-$POSTGRES_USER}

# postgres root configuration
export PGDATABASE=${PGDATABASE:-$POSTGRES_DB}
export PGUSER=${PGUSER:-$POSTGRES_USER}
export PGPASSWORD=${PGPASSWORD:-$POSTGRES_PASSWORD}

# download specified version of message-db
mkdir -p /tmp/message-db
wget -O /tmp/message-db/archive.tar.gz https://github.com/message-db/message-db/archive/refs/tags/$MDBVERSION.tar.gz
tar --extract --file /tmp/message-db/archive.tar.gz --directory /tmp/message-db --strip-components 1
rm /tmp/message-db/archive.tar.gz

# install message-db into specified database
DATABASE_NAME=$PGDATABASE CREATE_DATABASE=off /tmp/message-db/database/install.sh

# configure message-db user and set their search_path to include message_store
psql -c "GRANT message_store TO \"$MDBUSER\""
psql -c "ALTER ROLE $MDBUSER SET search_path = public,message_store"
