// Package trigger 拉起一轮“立即同步”（入库管道详设 14.4）。同步由调度方定时拉起，connectors 自己不常驻同步，
// 所以立即同步要经调度方：Kubernetes 实现从数据源的 CronJob 模板创建一个 Job（与 kubectl create job --from=cronjob 相同）。
// 没有接入调度时用 Unconfigured，接口返回 FAILED_PRECONDITION。
package trigger

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// SourceLabel 标出 CronJob 同步的是哪个数据源；立即同步按它找 CronJob，拉起的 Job 也带上它。
const SourceLabel = "bambuflow.io/source-id"

var (
	// ErrUnconfigured：部署没有接入调度。
	ErrUnconfigured = errors.New("没有接入调度，不能立即同步")
	// ErrNoSchedule：这个数据源没有定时同步任务。
	ErrNoSchedule = errors.New("这个数据源没有定时同步任务")
	// ErrRunning：已有进行中的同步。
	ErrRunning = errors.New("已有进行中的同步")
)

type Trigger interface {
	// Sync 拉起一轮同步，返回任务名。
	Sync(ctx context.Context, sourceID string) (string, error)
}

type Unconfigured struct{}

func (Unconfigured) Sync(context.Context, string) (string, error) { return "", ErrUnconfigured }

// Kubernetes 经 API server 操作本命名空间的 CronJob 与 Job；服务账号只需要 cronjobs 的 list 与 jobs 的 list、create。
type Kubernetes struct {
	Base      string // https://host:port
	Namespace string
	Token     func() (string, error)
	HTTP      *http.Client
}

const serviceAccount = "/var/run/secrets/kubernetes.io/serviceaccount/"

// InCluster 用 Pod 的服务账号连接 API server。
func InCluster() (*Kubernetes, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("不在 Kubernetes 集群内")
	}
	ns, err := os.ReadFile(serviceAccount + "namespace")
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(serviceAccount + "ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("读不出集群 CA")
	}
	return &Kubernetes{
		Base: "https://" + host + ":" + port, Namespace: strings.TrimSpace(string(ns)),
		// 令牌会轮换：每次请求重新读
		Token: func() (string, error) {
			b, err := os.ReadFile(serviceAccount + "token")
			return strings.TrimSpace(string(b)), err
		},
		HTTP: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}},
	}, nil
}

type cronJobList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			JobTemplate struct {
				Metadata struct {
					Labels map[string]string `json:"labels"`
				} `json:"metadata"`
				Spec json.RawMessage `json:"spec"`
			} `json:"jobTemplate"`
		} `json:"spec"`
	} `json:"items"`
}

type jobList struct {
	Items []struct {
		Status struct {
			Active int `json:"active"`
		} `json:"status"`
	} `json:"items"`
}

func (k *Kubernetes) Sync(ctx context.Context, sourceID string) (string, error) {
	selector := url.Values{"labelSelector": {SourceLabel + "=" + sourceID}}.Encode()
	var crons cronJobList
	if err := k.do(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+k.Namespace+"/cronjobs?"+selector, nil, &crons); err != nil {
		return "", err
	}
	switch len(crons.Items) {
	case 0:
		return "", ErrNoSchedule
	case 1:
	default:
		return "", fmt.Errorf("数据源 %s 有 %d 个定时同步任务，应只有一个", sourceID, len(crons.Items))
	}
	// 与定时任务的 concurrencyPolicy 一样不并发：同一数据源的两轮同步会重复发出变更
	var jobs jobList
	if err := k.do(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+k.Namespace+"/jobs?"+selector, nil, &jobs); err != nil {
		return "", err
	}
	for _, j := range jobs.Items {
		if j.Status.Active > 0 {
			return "", ErrRunning
		}
	}
	cj := crons.Items[0]
	labels := map[string]string{}
	for k, v := range cj.Spec.JobTemplate.Metadata.Labels {
		labels[k] = v
	}
	labels[SourceLabel] = sourceID
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// Job 名最长 63 个字符
	name := cj.Metadata.Name
	if len(name) > 63-len("-manual-")-6 {
		name = name[:63-len("-manual-")-6]
	}
	name += "-manual-" + hex.EncodeToString(b)
	job := map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": map[string]any{"name": name, "labels": labels,
			"annotations": map[string]string{"cronjob.kubernetes.io/instantiate": "manual"}},
		"spec": cj.Spec.JobTemplate.Spec,
	}
	if err := k.do(ctx, http.MethodPost, "/apis/batch/v1/namespaces/"+k.Namespace+"/jobs", job, nil); err != nil {
		return "", err
	}
	return name, nil
}

func (k *Kubernetes) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, k.Base+path, body)
	if err != nil {
		return err
	}
	token, err := k.Token()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := k.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		// 只记状态码：API server 的错误信息里可能带内部资源名，不往外透
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("kubernetes API %s %s 返回 %d", method, strings.SplitN(path, "?", 2)[0], resp.StatusCode)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(out)
}
