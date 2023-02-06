FROM timescale/timescaledb-ha:pg14.6-ts2.9.2-latest

ENV POSTGIS_VERSION 3.3.2
# ENV POSTGIS_SHA256 dfcbad0c6090c80bc59d3ea77d1adc4b3ade533a403761b4af6d9a44be1a6e48

USER root

RUN apt update

ENV build_deps ca-certificates \
  git \
  build-essential \
  libpq-dev \
  postgresql-server-dev-14 \
  curl \
  libreadline6-dev \
  zlib1g-dev \
  postgresql-14-unit

RUN apt-get install -y --no-install-recommends $build_deps pkg-config cmake

WORKDIR /home/pg_graphql

ENV HOME=/home/pg_graphql \
  PATH=/home/pg_graphql/.cargo/bin:$PATH
ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN chown postgres:postgres /home/pg_graphql

USER postgres

RUN \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal --default-toolchain nightly &&\
    # source "$HOME/.cargo/env" &&\
    rustup --version && \
    rustc --version && \
    cargo --version &&\
    cargo install cargo-pgx --version 0.6.1 --locked &&\
    cargo pgx init --pg14 $(which pg_config) &&\
    git clone https://github.com/supabase/pg_graphql.git &&\
    mv pg_graphql/* ./
    # chmod a+rwx pg_graphql &&\
    # cd pg_graphql

USER root
RUN cargo pgx install

USER postgres

COPY ./istsos_schema.sql /docker-entrypoint-initdb.d/11_init_db_istsos_schema.sql
