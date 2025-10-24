// log_collector/model.go
package main

import "time"

// IngressLog representa un registro de log de Nginx/Ingress.
type IngressLog struct {
	Timestamp    time.Time `ch:"timestamp" json:"timestamp"`
	Level        string    `ch:"log_level" json:"log_level"`
	Message      string    `ch:"message" json:"message"`
	K8sNamespace string    `ch:"k8s_namespace" json:"k8s_namespace"`
	K8sPodName   string    `ch:"k8s_pod_name" json:"k8s_pod_name"`
	ServiceHost  string    `ch:"service_host" json:"service_host"`
	HTTPStatus   uint16    `ch:"http_status" json:"http_status"`
	RequestTime  float64   `ch:"request_time" json:"request_time"`
}
