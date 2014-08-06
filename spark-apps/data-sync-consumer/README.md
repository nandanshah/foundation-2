This project implements Spark based RabbitMQ Message consumers as standalone application.

Note: This project will not build from command line as it has dependency on Platform project.
      It can be built using eclipse by adding platform project as dependency.
	
Also, it is required that this program is up and running to pass RabbitMQ related JUnits [RabbitMQConnectorTest] from platform project.
	
How to run:
1. Start Cassandra. [Update src\main\resources\CassandraUpdater.properties file accordingly]
2. Start this project [Main class is App.java]
Queue listeners will start running in Spark Context.
