FROM ruby:2.7.2

RUN apt-get update -q && \
    apt-get -qy install sqlite3 libsqlite3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /app
WORKDIR /app

COPY Gemfile /app
RUN bundle install
