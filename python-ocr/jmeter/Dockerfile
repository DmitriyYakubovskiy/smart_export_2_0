FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ARG JMETER_VERSION=5.6.3
RUN curl -fsSL --retry 3 "https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz" -o /tmp/jmeter.tgz \
    && echo '5978a1a35edb5a7d428e270564ff49d2b1b257a65e17a759d259a9283fc17093e522fe46f474a043864aea6910683486340706d745fcdf3db1505fd71e689083  /tmp/jmeter.tgz' | sha512sum -c - \
    && tar -xzf /tmp/jmeter.tgz -C /opt && rm /tmp/jmeter.tgz \
    && ln -s "/opt/apache-jmeter-${JMETER_VERSION}" /opt/jmeter
ENV PATH="/opt/jmeter/bin:${PATH}" HEAP="-Xms256m -Xmx1g"
WORKDIR /tests
COPY upload.jmx /tests/upload.jmx
COPY run.sh /tests/run.sh
# Git checkouts on Windows may use CRLF.
RUN sed -i 's/\r$//' /tests/run.sh
ENTRYPOINT ["sh", "/tests/run.sh"]
