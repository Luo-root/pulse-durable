// distributed 包定义实验性的可移植任务与协调契约。
// 不包含数据库后端，也不启动工作循环。
package distributed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

const (
	Protocol           = 1
	PayloadFormat      = 1
	MaxTextBytes       = 256
	MaxManifestEntries = 4096
	MaxPayloadBytes    = 4 << 20
	MaxEnvelopeBytes   = 8 << 20
)

var (
	ErrInvalid = errors.New("distributed: invalid task")
	// ErrInvalidRecord 表示 LoadOwned 在权威所有权和时钟检查后
	// 观察到的确定性损坏。它仍属于 ErrInvalid。
	ErrInvalidRecord    = fmt.Errorf("distributed: invalid owned record: %w", ErrInvalid)
	ErrMismatch         = errors.New("distributed: incompatible task")
	ErrDuplicate        = errors.New("distributed: duplicate registration")
	ErrSubmitConflict   = errors.New("distributed: submission conflict")
	ErrNotFound         = errors.New("distributed: task not found")
	ErrLeaseLost        = errors.New("distributed: lease lost")
	ErrRevisionConflict = errors.New("distributed: revision conflict")
	ErrTerminal         = errors.New("distributed: terminal task")
	ErrUnknown          = errors.New("distributed: operation commit unknown")
	ErrClockRegression  = errors.New("distributed: database clock regression")
)

type Identity struct {
	Namespace string `json:"namespace"`
	RunID     string `json:"run_id"`
}

// Route 是精确能力，不是版本范围。ManifestDigest 覆盖
// 完整有序的图键清单，包括内部键和流键。
type Route struct {
	TaskType        string `json:"task_type"`
	BusinessVersion string `json:"business_version"`
	Definition      string `json:"definition"`
	ManifestDigest  string `json:"manifest_digest"`
	Protocol        int    `json:"protocol"`
	RecordFormat    int    `json:"record_format"`
	PayloadFormat   int    `json:"payload_format"`
}

type descriptor struct {
	Identity      Identity               `json:"identity"`
	Route         Route                  `json:"route"`
	Manifest      []durable.CodecBinding `json:"codec_manifest"`
	Inputs        string                 `json:"inputs"`
	Payload       []byte                 `json:"payload"`
	PayloadDigest string                 `json:"payload_digest"`
}

// Task 持有规范化的不可变快照。访问器返回副本；零值无效。
// 线格式：encoding/json 固定字段顺序结构体、无空白；清单按各绑定的
// 规范 JSON 字节排序；载荷为 base64；SHA-256 为小写十六进制。
// 通过解码后逐字节比较，拒绝未知或重复字段、不同空白或转义以及尾随数据。
// 编解码器字节本身必须具有确定性。
type Task struct {
	wire   string
	digest string
}

func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func text(s string) bool   { return len(s) > 0 && len(s) <= MaxTextBytes && utf8.ValidString(s) }
func fingerprint(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

// NewTask 准备实际输入，不调用 Run、存储或执行业务。
func NewTask(ctx context.Context, id Identity, taskType, version string, g *pulse.Graph, in pulse.Inputs, codecs *durable.Codecs) (Task, error) {
	if !text(id.Namespace) || !text(id.RunID) || !text(taskType) || !text(version) {
		return Task{}, ErrInvalid
	}
	p, err := durable.Prepare(ctx, g, in, codecs)
	if err != nil {
		return Task{}, ErrInvalid
	}
	m, _ := json.Marshal(p.CodecManifest)
	d := descriptor{id, Route{taskType, version, p.Definition, hash(m), Protocol, durable.RecordFormat, PayloadFormat}, p.CodecManifest, p.Inputs, p.Payload, hash(p.Payload)}
	wire, err := json.Marshal(d)
	if err != nil {
		return Task{}, ErrInvalid
	}
	return ParseTask(wire)
}

// ParseTask 在任何构造函数或编解码器运行前限制并验证封装。
// 它检查结构和校验和，不检查本地图与编解码器兼容性：Registry
// 在向执行桥返回图和输入前验证后者。
func ParseTask(wire []byte) (Task, error) {
	if len(wire) == 0 || len(wire) > MaxEnvelopeBytes || !utf8.Valid(wire) {
		return Task{}, ErrInvalid
	}
	var d descriptor
	dec := json.NewDecoder(bytes.NewReader(wire))
	dec.DisallowUnknownFields()
	if dec.Decode(&d) != nil {
		return Task{}, ErrInvalid
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(wire, canonical) {
		return Task{}, ErrInvalid
	}
	r := d.Route
	if !text(d.Identity.Namespace) || !text(d.Identity.RunID) || !text(r.TaskType) || !text(r.BusinessVersion) || r.Protocol != Protocol || r.PayloadFormat != PayloadFormat || r.RecordFormat != durable.RecordFormat || !fingerprint(r.Definition) || !fingerprint(d.Inputs) || !fingerprint(r.ManifestDigest) || !fingerprint(d.PayloadDigest) {
		return Task{}, ErrInvalid
	}
	if len(d.Payload) > MaxPayloadBytes || len(d.Manifest) > MaxManifestEntries || d.Manifest == nil || hash(d.Payload) != d.PayloadDigest {
		return Task{}, ErrInvalid
	}
	previous := []byte(nil)
	for _, b := range d.Manifest {
		if !text(b.Graph) || len(b.Path) > MaxTextBytes || !utf8.ValidString(b.Path) || !text(b.Name) || !text(b.TypeID) || !text(b.Version) {
			return Task{}, ErrInvalid
		}
		encoded, _ := json.Marshal(b)
		if previous != nil && bytes.Compare(previous, encoded) >= 0 {
			return Task{}, ErrInvalid
		}
		previous = encoded
	}
	m, _ := json.Marshal(d.Manifest)
	if hash(m) != r.ManifestDigest {
		return Task{}, ErrInvalid
	}
	return Task{string(wire), hash(wire)}, nil
}
func (t Task) data() descriptor                      { var d descriptor; _ = json.Unmarshal([]byte(t.wire), &d); return d }
func (t Task) Identity() Identity                    { return t.data().Identity }
func (t Task) Route() Route                          { return t.data().Route }
func (t Task) Bytes() []byte                         { return []byte(t.wire) }
func (t Task) SubmissionDigest() string              { return t.digest }
func (t Task) Payload() []byte                       { return t.data().Payload }
func (t Task) InputsFingerprint() string             { return t.data().Inputs }
func (t Task) CodecManifest() []durable.CodecBinding { return t.data().Manifest }

// CompareSubmission 同时要求摘要和精确规范字段（线格式）相等。
// 后端必须在身份唯一约束保护下，与已有权威行调用此函数比较；
// 遇到重复项绝不替换该行或重置状态及检查点。
func CompareSubmission(existing, submitted Task) (bool, error) {
	a, e := ParseTask(existing.Bytes())
	if e != nil {
		return false, e
	}
	b, e := ParseTask(submitted.Bytes())
	if e != nil {
		return false, e
	}
	if a.Identity() != b.Identity() {
		return false, ErrInvalid
	}
	if a.digest != b.digest || a.wire != b.wire {
		return false, ErrSubmitConflict
	}
	return true, nil
}
