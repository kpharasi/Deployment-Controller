package main

import (
    "fmt"
    "log"
    "os"
    "path/filepath"
    "time"

    apps_v1 "k8s.io/api/apps/v1"
    "k8s.io/api/core/v1"
    meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/fields"
    "k8s.io/apimachinery/pkg/util/runtime"
    "k8s.io/apimachinery/pkg/util/wait"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/cache"
    "k8s.io/client-go/tools/clientcmd"
    "k8s.io/client-go/util/workqueue"
)

// Controller - Our custom controller struct
type Controller struct {
    indexer  cache.Indexer
    queue    workqueue.RateLimitingInterface
    informer cache.Controller
}

// NewController - constructor of a new Controller type
func NewController(queue workqueue.RateLimitingInterface, indexer cache.Indexer, informer cache.Controller) *Controller {
    return &Controller{
        informer: informer,
        indexer:  indexer,
        queue:    queue,
    }
}

// Run is a function to start the controller
func (c *Controller) Run(threadiness int, stopCh chan struct{}) {
    defer runtime.HandleCrash()

    // stop workers when done
    defer c.queue.ShutDown()
    log.Print("Starting Pod Controller")

    go c.informer.Run(stopCh)

    // sync of cache
    if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
        runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
        return
    }

    for i := 0; i < threadiness; i++ {
        go wait.Until(c.runWorker, time.Second, stopCh)
    }

    <-stopCh
    log.Print("Stopping Pod controller")
}

func (c *Controller) processNextItem() bool {
    // Wait for a new item in the working queue
    key, quit := c.queue.Get()
    if quit {
        return false
    }
    // Marks as done and unblocks the key for other workers
    defer c.queue.Done(key)

    // Invoke business logic
    err := c.syncToStdout(key.(string))
    // Handle the error if something errors out during the business logic
    c.handleErr(err, key)
    return true
}

// syncToStdout is the business logic of the controller
func (c *Controller) syncToStdout(key string) error {
    obj, exists, err := c.indexer.GetByKey(key)
    if err != nil {
        log.Printf("Fetching object with key %s from store failed with %v", key, err)
        return err
    }

    if !exists {
        // Below we will warm up our cache with a Pod, so that we will see a delete for one pod
        fmt.Printf("Deployment %s does not exist anymore\n", key)
    } else {

        desReplicas := obj.(*apps_v1.Deployment).Status.Replicas
        availReplicas := obj.(*apps_v1.Deployment).Status.AvailableReplicas

        fmt.Printf("Sync/Add/Update for Deployment %s. The desired condition is %t\n", obj.(*apps_v1.Deployment).GetName(), desReplicas == availReplicas)
    }
    return nil
}

// handleErr checks if an error happened and makes sure to retry
func (c *Controller) handleErr(err error, key interface{}) {
    if err == nil {
        // Forget about the history of the key on every successful synchronization
        c.queue.Forget(key)
        return
    }

    // This controller retries 5 times after which it stops trying.
    if c.queue.NumRequeues(key) < 5 {
        log.Printf("Error syncing pod %v: %v", key, err)

        // Re-enqueue the key rate limited.
        c.queue.AddRateLimited(key)
        return
    }

    c.queue.Forget(key)
    runtime.HandleError(err)
    log.Printf("Dropping pod %q out of the queue: %v", key, err)
}

func (c *Controller) runWorker() {
    for c.processNextItem() {
    }
}

func main() {
    // Getting kube config
    kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
    config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
    if err != nil {
        panic(err.Error())
    }

    // Building client set from config
    clienset, err := kubernetes.NewForConfig(config)
    if err != nil {
        panic(err.Error())
    }

    deploymentListWatcher := cache.NewListWatchFromClient(clienset.AppsV1().RESTClient(), "deployments", v1.NamespaceDefault, fields.Everything())

    // Creating the workqueue
    queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

    // Binding workqueue to cache
    indexer, informer := cache.NewIndexerInformer(deploymentListWatcher, &apps_v1.Deployment{}, 0, cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            key, err := cache.MetaNamespaceKeyFunc(obj)
            if err == nil {
                queue.Add(key)
            }
        },
        UpdateFunc: func(old interface{}, new interface{}) {
            key, err := cache.MetaNamespaceKeyFunc(new)
            if err == nil {
                queue.Add(key)
            }
        },
        DeleteFunc: func(obj interface{}) {
            key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
            if err == nil {
                queue.Add(key)
            }
        },
    }, cache.Indexers{})

    // Creating a new custom controller
    controller := NewController(queue, indexer, informer)

    // Warming up the cache
    indexer.Add(&apps_v1.Deployment{
        ObjectMeta: meta_v1.ObjectMeta{
            Name:      "mydeployment",
            Namespace: v1.NamespaceDefault,
        },
    })

    // Starting the controller
    stop := make(chan struct{})
    defer close(stop)
    go controller.Run(2, stop)

    // Wait forever
    select {}
}

