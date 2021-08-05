FROM ruby:2.7.2

RUN apt-get update -q && \
    apt-get -qy install sqlite3 libsqlite3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /app
WORKDIR /app

COPY Gemfile /app
RUN bundle install

RUN cd /tmp && \
    git clone https://github.com/pacbard/webclient.git && \
    cd webclient/fetcher && \
    gem install hoe && \
    rake gem && \
    gem install pkg/fetcher-0.4.5.gem && \
    cd ../../ && \
    rm -rf webclient
