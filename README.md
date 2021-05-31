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
   

## Example Launcher Scripts

We've put all launcher go files in their own directory with a go.mod looking like this:

    module inboxbooster.com/cloudfunction

The launcher file must be named function.go

    ehsmeng> ls -l webhook_prod
    total 8
    -rw-rw-r-- 1 ehsmeng ehsmeng 893 May 25 16:06 function.go
    -rw-rw-r-- 1 ehsmeng ehsmeng  39 May 25 10:04 go.mod

### webhook_prod

The webhook listener that receives events from the ESP.

    package p
   
    import (
        "github.com/manycore-com/forwarder/webhook"
        "net/http"
        "os"
    )
   
    func LauncherWebhookProd(w http.ResponseWriter, r *http.Request) {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("OUT_QUEUE_TOPIC_ID", "PROJECT_PROD_RESPONDER")
        os.Setenv("SIMPLE_HASH_PASSWORD", "aPassword")
   
        webhook.F(w, r)
    }

To deploy:<br>
gcloud functions deploy LauncherWebhookProd --runtime go113 --trigger-http --allow-unauthenticated --entry-point=LauncherWebhookProd --service-account=prod@project.iam.gserviceaccount.com --timeout=6 --clear-env-vars --memory=128MB --source=webhook_prod

Note: --source=webhook_proj means launch_webhook_prod.go and go.mod are in a
subdirectory called webhook_prod.

Note: We call Topics and Subscriptions the same, unless it's a subscription
auto generated by Cloud Scheduler.

### fanout_prod

The webhook listener does a bare minium. It checks that the insecure hash looks
ok and puts the message on the topic specified by **OUT_QUEUE_TOPIC_ID**. 
This minimizes the impact on the system if say the db goes down. We also
don't want to spend seconds forwarding every message here, that would be very 
costly. Instead, we forward a bunch of messages in parallel later.

Fanout checks the secure hash, checks user credentials, forward endpoints, and
puts one copy of the message per endpoint on appropriate queue.

    package p
    
    import (
        "context"
        forwarderFanout "github.com/manycore-com/forwarder/fanout"
        forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
        "os"
    )
    
    func FanoutProd(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("IN_SUBSCRIPTION_ID", "PROJECT_PROD_RESPONDER")
        os.Setenv("OUT_QUEUE_TOPIC_ID", "PROJECT_PROD_FORWARD1")
        os.Setenv("DB_USER", "username")
        os.Setenv("DB_PASS", "password")
        os.Setenv("INSTANCE_CONNECTION_NAME", "project:us-central1:dbname")
        os.Setenv("DB_NAME", "dbname")
    
        os.Setenv("NBR_ACK_WORKER", "32")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")  // Note: Keep in sync with trigger!
        os.Setenv("MAX_PUBSUB_QUEUE_IDLE_MS", "1200")
    
        return forwarderFanout.Fanout(ctx, m)
    }

To deploy:<br>
gcloud functions deploy FanoutProd --runtime go113 --max-instances=2 --trigger-topic=PROJECT_PROD_RESPONDER_TRG --entry-point=FanoutProd --service-account=prod@project.iam.gserviceaccount.com --timeout=60 --clear-env-vars --memory=256MB --source=fanout_prod

### forward_prod_queue1

This does the actual forward work. If it fails, there are two more attempts. 
Those cloud functions are identical copies of this one.

    package p
    
    import (
        "context"
        forwarderForward "github.com/manycore-com/forwarder/forward"
        forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
        "os"
    )
    
    func ForwardProdQueue1(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("AT_QUEUE", "1")
        os.Setenv("IN_SUBSCRIPTION_ID", "PROJECT_PROD_FORWARD1")
        os.Setenv("OUT_QUEUE_TOPIC_ID", "PROJECT_PROD_FORWARD2")
    
        os.Setenv("DB_USER", "username")
        os.Setenv("DB_PASS", "password")
        os.Setenv("INSTANCE_CONNECTION_NAME", "project:us-central1:dbname")
        os.Setenv("DB_NAME", "dbname")
    
        os.Setenv("MIN_AGE_SECS", "0")
        os.Setenv("NBR_ACK_WORKER", "32")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
        os.Setenv("MAX_PUBSUB_QUEUE_IDLE_MS", "1200")
    
        return forwarderForward.Forward(ctx, m)
    }

To deploy:<br>
gcloud functions deploy ForwardProdQueue1 --runtime go113 --max-instances=1 --trigger-topic=PROJECT_PROD_TRIGGER_FWD1 --entry-point=ForwardProdQueue1 --service-account=prod@project.iam.gserviceaccount.com --timeout=180 --clear-env-vars --memory=256MB --source=forward_prod_queue1

### forward_prod_queue2

This is retry 1/2 if the first forward attempt failed. Note the **AT_QUEUE**
variable. 1 is first attempt after fanout, 2 is second attempt.

    package p
    
    import (
        "context"
        forwarderForward "github.com/manycore-com/forwarder/forward"
        forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
        "os"
    )
    
    func ForwardProdQueue2(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("AT_QUEUE", "2")
        os.Setenv("IN_SUBSCRIPTION_ID", "PROJECT_PROD_FORWARD2")
        os.Setenv("OUT_QUEUE_TOPIC_ID", "PROJECT_PROD_FORWARD3")
    
        os.Setenv("DB_USER", "username")
        os.Setenv("DB_PASS", "password")
        os.Setenv("INSTANCE_CONNECTION_NAME", "project:us-central1:dbname")
        os.Setenv("DB_NAME", "dbname")
    
        os.Setenv("MIN_AGE_SECS", "0")
        os.Setenv("NBR_ACK_WORKER", "32")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
        os.Setenv("MAX_PUBSUB_QUEUE_IDLE_MS", "1200")
    
        return forwarderForward.Forward(ctx, m)
    }

To deploy:<br>
gcloud functions deploy ForwardProdQueue2 --runtime go113 --max-instances=1 --trigger-topic=PROJECT_PROD_TRIGGER_FWD2 --entry-point=ForwardProdQueue2 --service-account=prod@project.iam.gserviceaccount.com --timeout=180 --clear-env-vars --memory=256MB --source=forward_prod_queue2

### forward_prod_queue3

This is the last attempt to send the message.

    package p
    
    import (
        "context"
        forwarderForward "github.com/manycore-com/forwarder/forward"
        forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
        "os"
    )
    
    func ForwardProdQueue3(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("AT_QUEUE", "3")  // last
        os.Setenv("IN_SUBSCRIPTION_ID", "PROJECT_PROD_FORWARD3")
        os.Setenv("OUT_QUEUE_TOPIC_ID", "")
    
        os.Setenv("DB_USER", "username")
        os.Setenv("DB_PASS", "password")
        os.Setenv("INSTANCE_CONNECTION_NAME", "project:us-central1:dbname")
        os.Setenv("DB_NAME", "dbname")
    
        os.Setenv("MIN_AGE_SECS", "0")
        os.Setenv("NBR_ACK_WORKER", "32")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
        os.Setenv("MAX_PUBSUB_QUEUE_IDLE_MS", "1200")
    
        return forwarderForward.Forward(ctx, m)
    }

To deploy:<br>
gcloud functions deploy ForwardProdQueue3 --runtime go113 --max-instances=1 --trigger-topic=PROJECT_PROD_TRIGGER_FWD3 --entry-point=ForwardProdQueue3 --service-account=prod@project.iam.gserviceaccount.com --timeout=180 --clear-env-vars --memory=256MB --source=forward_prod_queue3
    
### trigger_fanout_prod

Fanout and Forward above both have two pubsub queues coming in to them. One with
actual messages, and the other one is just to trigger it.

By default, pubsub triggered cloud functions consume one message at a time. In
the case of forward, we're not really doing any CPU work, we're just transferring
messages. So the VM will just be sleeping on io wait.

Instead, we have "trigger functions" that wakes up by Cloud Scheduler. They check
the number of forward messages a cloud function has, and they check how many
unprocessed trigger messages the cloud function already has in its trigger queue.

With this information, it makes sure there is one trigger message per 
**MAX_NBR_MESSAGES_POLLED**. The cloud function wakes up on a trigger message
and tries to consume **MAX_NBR_MESSAGES_POLLED** (default 64) messages and forward
**NBR_PUBLISH_WORKER** at a time.

This way, instead of paying for one vm to wait 3s while forwarding one message,
you can forward 32 messages at the same time.

Note: **TRIGGER_SUBSCRIPTION_ID** is the subscription that was generated when
you deployed Fanout. As you surely remember, you had the flag 
--trigger-topic=PROJECT_PROD_RESPONDER_TRG and that made GCP generate a subject.

This function runs often. E.g every minute, every 2nd minute, or every 4th minute.
Whichever you see fit. It executes in a few seconds, and it does not add surplus 
trigger messages.

    package p
    
    import (
        "context"
        forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
        forwarderTrigger "github.com/manycore-com/forwarder/trigger"
        "os"
    )
    
    func TriggerFanoutProd(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("SUBSCRIPTION_TO_PROCESS", "PROJECT_PROD_RESPONDER")
        os.Setenv("TRIGGER_TOPIC", "PROJECT_PROD_RESPONDER_TRG")
        os.Setenv("TRIGGER_SUBSCRIPTION_ID", "gcf-FanoutProd-us-central1-PROJECT_PROD_RESPONDER_TRG")
    
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
    
        return forwarderTrigger.Trigger(ctx, m)
    }

To deploy:<br>
gcloud functions deploy TriggerFanoutProd --runtime go113 --max-instances=4 --trigger-topic=PROJECT_PROD_TRIGGER_FANOUT_TRG --entry-point=TriggerFanoutProd --service-account=prod@project.iam.gserviceaccount.com --timeout=120 --clear-env-vars --memory=256MB --source=trigger_fanout_prod

### trigger_forward_prod_queue1

This function triggers forward_prod_queue1 and makes sure the first forward attempt happens.

This function runs often. E.g every minute, every 2nd minute, or every 4th minute.
Whichever you see fit. It executes in a few seconds, and it does not add surplus
trigger messages.

Trigger this 

    package p
    
    import (
    "context"
    forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
    forwarderTrigger "github.com/manycore-com/forwarder/trigger"
    "os"
    )
    
    func TriggerForwardProd1(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("SUBSCRIPTION_TO_PROCESS", "PROJECT_PROD_FORWARD1")
        os.Setenv("TRIGGER_TOPIC", "PROJECT_PROD_TRIGGER_FWD1")
        os.Setenv("TRIGGER_SUBSCRIPTION_ID", "gcf-ForwardProdQueue1-us-central1-PROJECT_PROD_TRIGGER_FWD1")
    
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
    
        return forwarderTrigger.Trigger(ctx, m)
   }

To deploy:<br>
gcloud functions deploy TriggerForwardProd1 --runtime go113 --max-instances=1 --trigger-topic=PROJECT_PROD_TRIGGER_FORWARD1_TRG --entry-point=TriggerForwardProd1 --service-account=prod@project.iam.gserviceaccount.com --timeout=120 --clear-env-vars --memory=256MB --source=trigger_forward_prod_queue1

### trigger_forward_prod_queue2

This is for the first retry.

We don't want to run this too often since we want some time before the first retry.
Every even hour is one idea.

    package p
    
    import (
        "context"
        forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
        forwarderTrigger "github.com/manycore-com/forwarder/trigger"
        "os"
    )
    
    func TriggerForwardProd2(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("SUBSCRIPTION_TO_PROCESS", "PROJECT_PROD_FORWARD2")
        os.Setenv("TRIGGER_TOPIC", "PROJECT_PROD_TRIGGER_FWD2")
        os.Setenv("TRIGGER_SUBSCRIPTION_ID", "gcf-ForwardProdQueue2-us-central1-PROJECT_PROD_TRIGGER_FWD2")
    
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
    
        return forwarderTrigger.Trigger(ctx, m)
    }

To deploy:<br>
gcloud functions deploy TriggerForwardProd2 --runtime go113 --max-instances=1 --trigger-topic=PROJECT_PROD_TRIGGER_FORWARD2_TRG --entry-point=TriggerForwardProd2 --service-account=prod@project.iam.gserviceaccount.com --timeout=120 --clear-env-vars --memory=256MB --source=trigger_forward_prod_queue2

### trigger_forward_proj_queue3

This is the last retry attempt. If it fails here, it's considered lost.

Run for example every odd hour.

    package p
    
    import (
        "context"
        forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
        forwarderTrigger "github.com/manycore-com/forwarder/trigger"
        "os"
    )
    
    func TriggerForwardProd3(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
        os.Setenv("DEV_OR_PROD", "prod")
        os.Setenv("PROJECT_ID", "project")
        os.Setenv("SUBSCRIPTION_TO_PROCESS", "PROJECT_PROD_FORWARD3")
        os.Setenv("TRIGGER_TOPIC", "PROJECT_PROD_TRIGGER_FWD3")
        os.Setenv("TRIGGER_SUBSCRIPTION_ID", "gcf-ForwardProdQueue3-us-central1-PROJECT_PROD_TRIGGER_FWD3")
    
        os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
        os.Setenv("NBR_PUBLISH_WORKER", "32")
    
        return forwarderTrigger.Trigger(ctx, m)
    }

To deploy:<br>
gcloud functions deploy TriggerForwardProd3 --runtime go113 --max-instances=1 --trigger-topic=PROJECT_PROD_TRIGGER_FORWARD3_TRG --entry-point=TriggerForwardProd3 --service-account=prod@project.iam.gserviceaccount.com --timeout=120 --clear-env-vars --memory=256MB --source=trigger_forward_prod_queue3
