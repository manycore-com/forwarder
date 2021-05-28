# Webhook Forwarder

## Introduction

The project goal is to forward webhooks from one endpoint to n others. We use it for our Inboxbooster project which is 
based on processing of ESP (Email Service Provider) webhooks.

Famously Sendgrid allows only one forward, so it is our way to fix it.


It's built for the GCP ecosystem using cloud functions.


It's licensed under the MIT license. tl;dr: You can use it in your closed source
project if you want. We appreciate
bug reports and contributions.

This is the webservices counterpart to
https://github.com/manycore-com/django-webhook-forwarder

## How to?

- To add support for a new webhook receiver, please look at the esp package and add it there.


#### Please note
* Environment variables are specified per cloud function. Some environment variables
  have different values for different cloud functions.

## Components

### mandatory environment variables
* **PROJECT_ID** this is your GCP project id.
* **DEV_OR_PROD** this is not strictly necessary to set. We use **dev** for when a 
  developer is testing, **devprod** for a complete live instance that is just for
  test, and **prod** for the actual production instances.
* **SIMPLE_HASH_PASSWORD** is used for the simple hash that is checked in
  the fanout package.

### optional environment variables
* **NBR_PUBLISH_WORKER** this is how many go-routines are busy with sending data.
  For the forwarder package you might want to have a constrained number since 
  the receiving end might be upset about too many connections. Default is 32.
  For the fanout package, you can have a higher number if you like.
* **MAX_NBR_MESSAGES_POLLED** specifies how many messages fanout and forward
  tries to receive.
* **NBR_ACK_WORKER** is the number of go routines that are concerned with sending ACK
  to received messages.

### webhook package

#### Overview
1. The ESP (Email Service Provider, e.g Sendgrid) posts a webhook event that
   the webhook package listens to.
   <br><br>
   The webhook package is the webhook web listener entry point.
   The ESP posts webhooks to a URL this package listens to.
   <br><br>
   The URL the ESP posts to ends in...
 
       /v1/sg/1/aaa/bbb/

   * v1 is the protocol version.
   * sg is what ESP the webhook originates from. Here it's "sg" for "Sendgrid"
   * 1 is the database user id for the client.
   * aaa is a hash calculated from the user id above and a site wide secret password.
     This is just to do a quick filter of the most obvious stuff.
     It's specified in environment variable **SIMPLE_HASH_PASSWORD**
   * bbb is a hash calculated using user specific data.
     It therefore requires used data to be read
2. The webhook package does a quick check it's valid, and then it pushes
   the package to a pubsub queue.
   <br><br>
   The package is put on a pubsub queue and this cloud function exits.
   <br><br>
   The topic id is specified in environment variable **OUT_QUEUE_TOPIC_ID**
   <br><br>
   Why is it constructed this way? We want to minimize the time a thread is spent
   on one webhook event because it's costly to process it one and one.

### fanout package

#### Overview
1. A bunch of messages are loaded at once from the pubsub subscription that the 
   webhook above posted to.
   <br><br>
   The bunch size is specified in environment variable **MAX_NBR_MESSAGES_POLLED**
   <br><br>
   Both the fanout package and the forward package below have two subscriptions
   related to the incoming side.
   <br><br>
   One pubsub subscription is simply the queue of webhook messages.
   <br><br>
   The second subscription is written to by the trigger package.
   <br><br>
   Packages written here is what's waking up fanout and forwarder.
   Thanks to this separation of the queue and trigger, each cloud function can
   process many events at a time.
2. We load user configuration for the users that has received packages.
   <br><br>
   Currently we load user data from postgres only.
   The next logical step is to load from redis instead, and allow parallel requests.
3. Using this configuration we can verify the hash called "bbb" above.
   Hash is unique per user. You can for example use a randomly generated uuid.
4. If this hash is not ok, or user is not active, we ignore hooks to that user.
5. For each message we have n endpoints specified in the user configuration.
   <br><br>
   We publish every message+endpoint combination to the first forward pubsub queue.
   <br><br>
   The topic id is specified in environment variable **IN_SUBSCRIPTION_ID**
   <br><br>
   The environment variable **NBR_PUBLISH_WORKER** controls how many parallel
   workers we use. Both in forwarder and fanout.

### forward package

#### Overview
1. The forward package does the actual forward. When waken up by a trigger message,
   it reads a number of messages (specified in environment variable 
   **MAX_NBR_MESSAGES_POLLED**) from a subscription 
   (named in environment variable **IN_SUBSCRIPTION_ID**).
   It also **optionally** checks that the package is at least as many seconds old
   as is specified in the environment variable **MIN_AGE_SECS**
2. It tries to forward to the url in the message.
   This is what has the potential to be the biggest time hog and is why we
   want to try to parallelize it.
   How parallel is specified in environment variable **NBR_PUBLISH_WORKER**
3. If there is a problem with the forward, the problem is either intermittent
   (like a http 500 response), or permanent. If it's intermittent, and if it's
   not the last retry, the message is pushed forward to the next queue.
   Next queue's topic is in environment variable **OUT_QUEUE_TOPIC_ID**
4. There are three forward attempts. The first is just after **fanout** has
   validated the messages and attached forward urls to them. If the third
   attempt fails, the forward message is lost.
   You tell the forwarder package which attempt it is by using the
   environment variable **AT_QUEUE**
   
### trigger package
The trigger package wakes up the fanout package, and the forward package.

#### Overview
1. A Cloud Scheduler pubsub trigger wakes up the trigger package.
2. Trigger package checks the subscription it should trigger how many
   messages it currently have. The subscription is named by
   environment variable **SUBSCRIPTION_TO_PROCESS**
3. The environment variable **MAX_NBR_MESSAGES_POLLED** says how many messages
   each forwarder or fanout cloud function are expected to consume.
4. We check the size of the trigger queue using the subscription name
   from environment variable **TRIGGER_SUBSCRIPTION_ID**
5. Trigger package will put 
   
   ceil(number of messages on queue / max_nbr_messages_polled) - number_of_items_already_on_trigger_queue
   
   ...on the trigger queue named by environment variable
   **TRIGGER_TOPIC**
   

