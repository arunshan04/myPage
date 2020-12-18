---
title:  "Akka Actors vs Streams for Rest APIs"
excerpt: "A comparison of use cases for Spray IO (on Akka Actors) and Akka Http (on Akka Streams) for creating rest APIs"
date:   2017-10-10 00:08:49 +0530
categories: scala
tags: scala akka akka-actors akka-streams akka-http spray-io
comments: true
---

This post is about a comparison between Spray IO and Akka Http, and specifically one of the major changes in the way we design code when using actor based frameworks.
Also a small comparison of choosing between Akka Actors and Akka Streams for development of applications and what to use where.

## Background

Before going deeper and comparing the advantages and disadvantages of the two frameworks and when to use what, let us first try and understand a few concepts from definitions. This section is by no means exhaustive and might require additional reading before one can gain a complete understanding.

### Concurrency
No it doesn't mean things should run in parallel. It means that if you were to break a program into smallest of fragments and execute these out of order, the results would be same.

This implies that individual operations should be stateless. (From the point of REST api, we can say that each request in a very simple service should not change the state (or behavior) of the service itself. - It can make changes to the database or some other external service though, which makes the overall application stateful)

This is different from parallel execution though, which means that two operations happen at the same time, in different CPUs. Concurrency means that the operations doesn't necessarily execute in parallel (at the same time).
[Wiki](https://en.wikipedia.org/wiki/Concurrency_(computer_science))

### Scala Futures
Futures are a way of defining a particular set of operations which will produce an output at a later stage after it has been executed as part of a different thread. The calling thread is not blocked during the execution of the Future. Futures allows us to define call backs where we can pass functions which will get executed after a future has been completed.

For a future to be executed we must define an execution context (or have it as an implicit). This is an abstraction over creating a Thread and running it. It offers a way to write cleaner code which abstracts out the creation and termination of Threads from the user.

### Akka Actors
Actor model is a programming model which is used to implement concurrency. Actors mean the isolation of state into different components and each component communicating with others using message passing over a queue. In the world of Akka and Scala, we must keep the following properties in mind:
 - Actors have a mutable state stored inside the actor.
 - The state is mutated by sending messages. Thus an actor has a mailbox where messages come, and by default it is a java ```ConcurrentLinkedQueue```. There can be other mailboxes as well.
 - The order of messages decides the order of execution. Remember, you are queueing your messages in a ConcurrentLinkedQueue for your Actor.
 - Processing of messages for one instance of the actor does not happen in parallel. It is concurrent by definition, not parallel. (This is the default behavior. Note that one mailbox can be shared by multiple instances of the same actor if configured - but that is not the default - and this needs a routing actor, which routes the messages to various instances of the actor, and this scheduling can be configured too.)
 - There is no shared mutable state across (different instances of) actors. Actors are self contained in their states.
 - Actors have hierarchy. Execution and state of actors are supervised by a supervisor actor.
 - Consumption of a message by an actor is determined by when the actor executes, this is in turn governed by the dispatcher or the execution context.
 - When an actor is scheduled to run, it gets a thread from the dispatcher and the thread for an actor is not reserved.
 - Akka doesn't provide guarantees on messages being consistent, and messages can be dropped if the mailbox is full. 
 
Thus we define actor as a state store that can be mutated by passing messages to it. It is definitely not some container for a deterministic stateless function which should be called for a response.

Why? Because that is what an object or a static function is used for. Since the function is deterministic, it doesn't change the state of an Actor, and can be called in parallel from different threads, it doesn't need to be concurrent.

### Akka Streams
Streams are a model of programming which is very functional in nature. It starts with having an input and making it flow through a series of operators or functions on the stream and producing an output. One example of streaming is the Unix ```|``` (pipe) operator. 

In most Http Servers that we build, we essentially write the function which maps requests to the responses. In most frameworks like Spring Boot or other MVC frameworks, one thread used to apply the transformation from a HttpRequest to a HttpResponse for every request to the server.

Akka Streams provide an abstraction over the Akka Actor model specifically in a way where we write the functions to be chained and executed on an input to produce an output. 

With this we are abstracted out of how the messages should be passed between actors, buffering, back pressure control and other components which largely remain same for most streaming applications using actors. This also helps us define complex transformations expressed as a directed acyclic graph, and not merely a simple function. Akka Streams API allows us to cleanly define async boundaries in a set of operations and create flows without worrying much about concurrency and execution. 

### Spray IO
Simply put, Spray IO is a framework where you create REST API to talk and interact with Akka Actors. Your Http Routing is always contained within an Actor.

Now since actors execute messages one at a time, it is very good for situations involving reads and writes where there is a state change that needs to be maintained and passed on to the next request.

For situations which need only reads or only writes where each request is truly concurrent, we don't need Actors.

### Akka Http
This is a framework for building Http Servers and Clients based on Akka Streams and Akka Actors.

The primary design change when moving from Spray to Akka Http is that Akka-http is based on Akka Streams and not directly on Akka Actors. Akka Streams internally use Akka Actors but that is an implementation level detail for akka streams.

This essentially means that akka http and spray behave differently even with the same user facing code.

## The REST API
With both Spray and Akka Http, defining REST apis is fairly simple. We define the route and then bind it to the port. It then starts accepting connections and responds. But there are a lot of differences in its implementations.

### Spray
We define the "route" as a trait extending HttpService of Spray Routing. This route is evaluated for each request to Spray. As an example lets see the following:

_Source code is [here](https://github.com/anish749/spray-io-vs-akka-http)_

For the purpose of examples in this post we would use a server which takes in a String and returns all possible permutations of the characters in the String.

I choose this function for this example because it has a time complexity of O(n!) which increases to a very high value with a small change in the input.
```scala
// Spray Routing trait
trait RestService extends HttpService with JsonSupport {
  implicit val log: LoggingAdapter
  implicit val executionContext: ExecutionContext

  val route =
    pathSingleSlash {
      get {
        complete {
          ServiceStatus("OK", "Welcome to Spray IO Rest Service")
        }
      }
    } ~ pathPrefix("sync") {
      pathPrefix("permutations") {
        get {
          parameters('string.as[String]) { (inputString) =>
            complete {
              new Permutations(inputString)
            }
          }
        }

      }
    }
}
```

Now we need to define our Router Actor, and override the receive method, to reply with the route that we have defined in our Service trait.

Thus the route is what gets executed when requests come to the actor. Where is the state of the actor? In this simple example, there is none. However, you can have the state inside the Router actor and this state can be updated concurrently with each request if the need arises.
```scala
// Spray Router Actor
class RestRouter(implicit val system: ActorSystem) extends Actor with RestService {
  implicit val executionContext = system.dispatcher
  implicit val log = Logging(system, getClass)

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  val actorRefFactory = context

  // Here we override the receive function of the Actor to execute our Rest Route
  val receive = runRoute(route)
}
```

Now that we have our service defined, we create the actors, and start the server by sending the Bind command to the Http extension.

This would start a ```HttpListener``` actor for a given port. When a request comes from client, it will first spawn a new ```HttpServerConnection``` actor. This actor is responsible for accepting the requests and sending them to our router actor which knows how to respond to a HttpRequest with a HttpResponse.

Now lets execute the code. Please refer to this [GitHub Repo](https://github.com/anish749/spray-io-vs-akka-http) for the code, if you want to execute yourself

Things to note. All requests are executed sequentially but in different threads. Remember your actors had a mailbox which is a Queue. And there is only one instance of the actor defined. We didn't define a set of actor instances and scheduler actor to give the received requests in parallel to different instances of the same actor.

Here it is easy to notice that the calls become slow as the characters in string increase, which is expected for the permutation function. However also note that requests are blocking in nature. Even though processing of requests can be made concurrent (We can process one request independent of another), the calls are blocking the server, and it cannot respond to another request concurrently.

_Where can this work:_
For cases where requests must be processed in order, where you do not need things to run in parallel. Cases where each requests modifies the state of the service and the modification should be visible to the subsequent request.
Eg: You have an API with which you want to submit a task, and check after sometime the status of it. However after submitting the task you want the server to start processing and the request to start should complete. There should be only one instance of the task running. So with a submit, you modify the state of the actor and then you query the actor using REST api for the status of the task.
An Implementation of this use case is there with Akka Http later on.

_Where will this be an anti pattern:_
If all your requests are stateless, and each request doesn't change the state of the service, this is an anti pattern. You can possibly run these requests in parallel, but with an Actor pattern and one instance (ActorRef) of the Actor defined, you can't run requests in different threads at the same time.

### Spray IO and responding HttpRequests with Futures
Is there a workaround? Say for cases where there will be a high throughput required, all requests are concurrent in nature, and might take a few seconds to execute. It should not block the caller while it is executing.

This is where we use Futures inside an actor. It should be noted that actors are not meant for running things in parallel. It is meant to store state, but in these cases our operations would be defined in a multi threaded way and not using actors, i.e., we won't have multiple actors which processes the request.

The route looks something like this:
```scala
pathPrefix("async") {
  pathPrefix("permutations") {
    get {
      parameters('string.as[String]) { (inputString) =>

        val respFut = Future {
          log.info("Processing request for {} in Future", inputString)
          new Permutations(inputString)
        }

        onComplete(respFut) {
          case Success(successPermutations: Permutations) => complete(successPermutations)
          case Failure(ex) =>
            log.error("Exception while processing : {}", ex.getMessage)
            complete(StatusCodes.InternalServerError, ServiceStatus("ERROR", "Some error in service"))
        }
      }
    }
  }
}
```
The code here takes an input string and returns all permutations by calculating that in a different thread.

We use the callback ```onComplete``` which takes in a Future. The router actor is no longer blocked while the Future is getting executed.

This makes requests to the server concurrent and non-blocking in nature.

### Akka Http 
An Http Layer running on top of Akka Streams is much different from what we saw earlier. Even though the Routing remains the same, the whole semantics of execution changes.

For this particular case itself we see that Akka Streams running requests concurrently just out of the box, without any coding with Futures.

This is because Akka Streams defines the operations as a DAG, with a source and sink. Actors has been abstracted here.

The code is exactly the same in terms of routing.

The code to start the server however changes. Here we don't have any actors and we don't instantiate any. We create an ActorSystem and an ActorMaterializer. 

We call the ```Http().bindAndHandle(route, host, port)``` and this gives us a binding future which will terminate once the server terminates.

Actors are used and created underneath for the server to work. For user facing code and implementations, we can have Actors, and akka http will work well that as well.

Here is an example where an user defined actor is used to store the state.

### Akka Http with Actors
Akka Http works perfectly well with actors defined by us. In the following example we create a Task Manager. 

<script src="https://gist.github.com/anish749/afe516ef0f96470cd381c1f5b0d79f59.js"></script>

It is responsible for creating and managing Tasks (these can be server side jobs). It is implemented using Actors, which stores the state of each Task.

Task itself is a simple class which starts and does some operation. Its state is stored inside the instance of the class and gets updated as the Task progresses. 

The TaskManager is an Actor which keeps the instances of Task and can query them to understand their state. This actor runs the requests concurrently and avoids all problems which would have arose because of multiple requests coming at the same time. All TaskCreation / Querying happens one at a time because the state of all the tasks is stored in the Actor.

Original Code in [Github Repo](https://github.com/anish749/spray-io-vs-akka-http/blob/master/akka-rest-service/src/main/scala/org/anish/akka/TaskManagerServer.scala)


## References
 - https://stackoverflow.com/questions/11675422/actor-pattern-what-it-exactly-constitutes
 - http://blog.michaelhamrah.com/2015/01/a-gentle-introduction-to-akka-streams/
 - https://www.chrisstucchio.com/blog/2013/actors_vs_futures.html
