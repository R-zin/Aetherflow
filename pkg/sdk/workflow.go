package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
)

type WorkflowFunc func(ctx WorkflowContext, input []byte) ([]byte, error)
type ActivityFunc func(ctx context.Context, input []byte) ([]byte, error)

type Registry struct {
	mu         sync.RWMutex
	workflows  map[string]WorkflowFunc
	activities map[string]ActivityFunc
}

var DefaultRegistry = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{
		workflows:  make(map[string]WorkflowFunc),
		activities: make(map[string]ActivityFunc),
	}
}

func (r *Registry) RegisterWorkflow(name string, fn WorkflowFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workflows[name] = fn
}

func (r *Registry) RegisterActivity(name string, fn ActivityFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.activities[name] = fn
}

func (r *Registry) GetWorkflow(name string) (WorkflowFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.workflows[name]
	return fn, ok
}

func (r *Registry) GetActivity(name string) (ActivityFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.activities[name]
	return fn, ok
}

func RegisterWorkflow(name string, fn WorkflowFunc) {
	DefaultRegistry.RegisterWorkflow(name, fn)
}

func RegisterActivity(name string, fn ActivityFunc) {
	DefaultRegistry.RegisterActivity(name, fn)
}

type ReplayResult struct {
	Commands     []*Command
	IsComplete   bool
	IsFailed     bool
	Output       []byte
	ErrorMessage string
}

type WorkflowRunner struct {
	registry *Registry
}

func NewWorkflowRunner(reg *Registry) *WorkflowRunner {
	if reg == nil {
		reg = DefaultRegistry
	}
	return &WorkflowRunner{registry: reg}
}

func (wr *WorkflowRunner) ExecuteOrReplay(
	workflowID, runID, workflowType string,
	input []byte,
	historyEvents []*v1.HistoryEvent,
) (*ReplayResult, error) {
	wfFn, ok := wr.registry.GetWorkflow(workflowType)
	if !ok {
		return nil, fmt.Errorf("workflow type %q is not registered", workflowType)
	}

	initialTime := time.Now()
	if len(historyEvents) > 0 && historyEvents[0].TimestampNs > 0 {
		initialTime = time.Unix(0, historyEvents[0].TimestampNs)
	}

	ctx := newWorkflowContext(workflowID, runID, historyEvents, initialTime)

	resultBytes, err := wfFn(ctx, input)
	if err != nil {
		if errors.Is(err, ErrReplayYield) {
			// Workflow yielded waiting for an asynchronous activity or timer to complete
			return &ReplayResult{
				Commands: ctx.scheduledCmds,
			}, nil
		}
		if errors.Is(err, ErrNonDeterministicWorkflow) {
			return nil, err
		}
		// Business or activity failure
		return &ReplayResult{
			IsFailed:     true,
			ErrorMessage: err.Error(),
			Commands: []*Command{
				{
					Type: CommandFailWorkflow,
					Payload: &v1.WorkflowExecutionFailedAttributes{
						ErrorMessage: err.Error(),
					},
					SeqNum: ctx.commandSeq + 1,
				},
			},
		}, nil
	}

	// Workflow completed successfully
	return &ReplayResult{
		IsComplete: true,
		Output:     resultBytes,
		Commands: []*Command{
			{
				Type: CommandCompleteWorkflow,
				Payload: &v1.WorkflowExecutionCompletedAttributes{
					ResultPayload: resultBytes,
				},
				SeqNum: ctx.commandSeq + 1,
			},
		},
	}, nil
}

// Helper to convert Go types to JSON bytes for inputs
func JSONPayload(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
