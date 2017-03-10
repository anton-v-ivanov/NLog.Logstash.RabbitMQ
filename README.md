# NLog target for Logstash

This extension is designed for writing logs of your .NET application to the [LogStash](http://logstash.net/) very fast and asynchronously.

##Configuration:

Minimum target configuration:
```xml
  <nlog throwExceptions="true">
    <extensions>
      <add assembly="NLog.Logstash.RabbitMQ" />
    </extensions>
    <targets>
      <target name="logstash"
            ConnectionString="amqp://guest:guest@logstash:5672"
            type="RabbitMQ"
            app="Name_Of_Your_Application"/>
    </targets>
    <rules>
      <logger name="*" minlevel="Debug" writeTo="logstash" />
    </rules>
  </nlog>
```
Complete configuration:
```xml
<nlog throwExceptions="true">
    <extensions>
      <add assembly="NLog.Logstash.RabbitMQ" />
    </extensions>
    <targets>
      <target name="logstash"
              ConnectionString="amqp://guest:guest@logstash:5672"
              app="Name_Of_Your_Application"
              type="RabbitMQ"
              Topic="${level}"
              Exchange="logging-exchange"
              Durable="true"
              RetryCount="4"
              RetryDelay="500"
              ConnectionRetryDelay="2000"
              DeliveryMode="Persistent"
              MaxMessageCount="10000"
            />
    </targets>
    <rules>
      <logger name="*" minlevel="Debug" writeTo="logstash" />
    </rules>
  </nlog>
```
* **ConnectionString** - is a string with connection information for RabbitMQ (username, password, host, port and vhost)
* **app** - name of your application
* **type** - must be "RabbitMQ"
* **Topic** - RabbitMQ routing_key param value. Default is "${level}" (Info, Error, Warn etc)
* **Exchange** - name of the exchange of RabbitMQ server
* **Durable** - defines if the exchange will be durable or not
* **RetryCount** - how much time we retry to dequeue the message from internal queue while waiting new messages
* **RetryDelay** - delay between dequeue retry attempts
* **ConnectionRetryDelay** - delay between attempts to restore connection to RabbitMQ if it's lost
* **DeliveryMode** - RabbitMQ delivery mode. 1 for NonPersistent and 2 for Persistent
* **MaxMessageCount** - size of internal queue. Default is 10000 messages
