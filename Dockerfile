FROM openjdk:11-jre-slim

COPY target/spark-operator-0.1.0-SNAPSHOT-jar-with-dependencies.jar /spark-operator/spark-operator-0.1.0.jar

ENTRYPOINT ["java", "-jar", "/spark-operator/spark-operator-0.1.0.jar"]
