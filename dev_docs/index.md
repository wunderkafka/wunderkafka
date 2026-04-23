# Welcome to Wunderkafka's documentation!

<br>

<figure>
    <blockquote style="margin: 0;">
        <p style="padding: 15px;background: #eee;border-radius: 5px;">The power of librdkafka for <s>humans</s> pythons</p>
    </blockquote>
</figure>

Wunderkafka provides a handful of facades for C-powered consumer/producer.
It's built on top of the [confluent-kafka](https://pypi.org/project/confluent-kafka/).

## Rationale

```txt
Das ist wunderbar!
```

## What are we about?

-   [Cloudera](https://www.cloudera.com/) installation with its own
    schema registry
-   [Apache Avro™](https://avro.apache.org/) is used
-   Installation requires features which are fully supported by
    [librdkafka](https://github.com/edenhill/librdkafka), but not
    bundled in confluent-kafka python wheel
-   Constant need to use producers and consumers, but without a one-screen boilerplate
-   Frequent need to consume not purely *events*, but *fairly recent events*
-   Frequent need to handle a large number of events

So, that's it.

If you suffer from the same problems, you may don't need to reinvent your own wheel; you can try ours.

## What about other projects?

Corresponding to [ASF wiki,](https://cwiki.apache.org/confluence/display/KAFKA/Clients#Clients-Python) there are plenty of python clients.

-   [confluent-kafka](https://pypi.org/project/confluent-kafka/) is a de-facto standard but doesn't work out-of-the-box for us, as mentioned above
-   [Kafka Python](https://github.com/dpkp/kafka-python) is awesome, but not as performant as confluent-kafka
-   pykafka [here](https://github.com/Parsely/pykafka) and [here](https://github.com/dsully/pykafka) looks unmaintained: has been archived
-   [pykafkap](https://github.com/urbanairship/pykafkap) has only producer and looks unmaintained: no updates since 2014
-   [brod](https://github.com/datadog/brod) is not maintained in favor of Kafka Python.
