FROM sbtscala/scala-sbt:eclipse-temurin-21.0.8_9_1.12.5_3.8.2

WORKDIR /app

COPY project ./project
COPY build.sbt ./
COPY src ./src

RUN sbt clean compile

EXPOSE 8081

CMD ["sbt", "run"]