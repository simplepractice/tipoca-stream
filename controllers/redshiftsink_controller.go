/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	consumer "github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"
)

// RedshiftSinkReconciler reconciles a RedshiftSink object
type RedshiftSinkReconciler struct {
	client.Client

	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	KafkaTopicRegexes *sync.Map
	KafkaWatcher      consumer.KafkaWatcher

	GitCache *sync.Map
}

// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *RedshiftSinkReconciler) fetchLatestTopics(
	regexes string) ([]string, error) {

	var topics []string
	var err error
	var rgx *regexp.Regexp
	topicsAppended := make(map[string]bool)
	expressions := strings.Split(regexes, ",")

	allTopics, err := r.KafkaWatcher.Topics()
	if err != nil {
		return topics, err
	}

	for _, expression := range expressions {
		rgxLoaded, ok := r.KafkaTopicRegexes.Load(expression)
		if !ok {
			rgx, err = regexp.Compile(strings.TrimSpace(expression))
			if err != nil {
				return topics, fmt.Errorf(
					"Compling regex: %s failed, err:%v\n", expression, err)
			}
			r.KafkaTopicRegexes.Store(expression, rgx)
		} else {
			rgx = rgxLoaded.(*regexp.Regexp)
		}

		for _, topic := range allTopics {
			if !rgx.MatchString(topic) {
				continue
			}
			_, ok := topicsAppended[topic]
			if ok {
				continue
			}
			topics = append(topics, topic)
			topicsAppended[topic] = true
		}
	}

	return topics, nil
}

func (r *RedshiftSinkReconciler) reconcile(
	ctx context.Context,
	rsk *tipocav1.RedshiftSink,
) (
	ctrl.Result,
	ReconcilerEvent,
	error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 30}

	kakfaTopics, err := r.fetchLatestTopics(rsk.Spec.KafkaTopicRegexes)
	if err != nil {
		return result, nil, err
	}

	masterSinkGroup := NewSinkGroup(
		"master", r.Client, r.Scheme, rsk, kakfaTopics, "")

	return masterSinkGroup.Reconcile(ctx)
}

func (r *RedshiftSinkReconciler) Reconcile(
	req ctrl.Request) (_ ctrl.Result, reterr error) {

	klog.Infof("Reconciling %+v", req)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var redshiftsink tipocav1.RedshiftSink
	err := r.Get(ctx, req.NamespacedName, &redshiftsink)
	if err != nil {
		return ctrl.Result{
			RequeueAfter: time.Second * 30}, client.IgnoreNotFound(err)
	}

	original := redshiftsink.DeepCopy()

	// Always attempt to patch the status after each reconciliation.
	defer func() {
		if reflect.DeepEqual(original.Status, redshiftsink.Status) {
			return
		}
		err := r.Client.Status().Patch(
			ctx,
			&redshiftsink,
			client.MergeFrom(original),
		)
		if err != nil {
			reterr = kerrors.NewAggregate(
				[]error{
					reterr,
					fmt.Errorf(
						"error while patching EtcdCluster.Status: %s ", err),
				},
			)
		}
	}()

	// Perform a reconcile, getting back the desired result, any utilerrors
	result, event, err := r.reconcile(ctx, &redshiftsink)
	if err != nil {
		err = fmt.Errorf("Failed to reconcile: %s", err)
	}

	// Finally, the event is used to generate a Kubernetes event by
	// calling `Record` and passing in the recorder.
	if event != nil {
		event.Record(r.Recorder)
	}

	return result, err
}

// SetupWithManager sets up the controller and applies all controller configs
func (r *RedshiftSinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tipocav1.RedshiftSink{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
