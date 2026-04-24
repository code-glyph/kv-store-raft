package integration

import (
	"sort"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"kv-store-raft/internal/kv"
)

type linearizabilityInput struct {
	Op    string
	Key   string
	Value string
}

type linearizabilityOutput struct {
	OK    bool
	Value string
}

func parseLinearizabilityInput(raw []byte) linearizabilityInput {
	cmd, err := kv.DecodeCommand(raw)
	if err != nil {
		return linearizabilityInput{Op: "invalid"}
	}
	return linearizabilityInput{
		Op:    cmd.Op,
		Key:   cmd.Key,
		Value: cmd.Value,
	}
}

func linearizabilityOutputValue(input linearizabilityInput, err error) string {
	if err != nil {
		return ""
	}
	switch input.Op {
	case kv.OpSet:
		return input.Value
	case kv.OpDelete:
		return ""
	default:
		return ""
	}
}

func assertLinearizableOperations(t *testing.T, ops []porcupine.Operation, timeout time.Duration) {
	t.Helper()
	result := porcupine.CheckOperationsTimeout(kvPorcupineModel(), ops, timeout)
	if result != porcupine.Ok {
		t.Fatalf("expected linearizability check result %s, got %s", porcupine.Ok, result)
	}
}

func kvPorcupineModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionOperationsByKey,
		Init: func() interface{} {
			return ""
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			in := input.(linearizabilityInput)
			out := output.(linearizabilityOutput)
			currentValue := state.(string)

			if !out.OK {
				return true, currentValue
			}

			switch in.Op {
			case kv.OpSet:
				if out.Value != in.Value {
					return false, currentValue
				}
				return true, in.Value
			case kv.OpDelete:
				if out.Value != "" {
					return false, currentValue
				}
				return true, ""
			default:
				return false, currentValue
			}
		},
		Equal: func(state1 interface{}, state2 interface{}) bool {
			return state1.(string) == state2.(string)
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			in := input.(linearizabilityInput)
			out := output.(linearizabilityOutput)
			return in.Op + "(" + in.Key + "," + in.Value + ") => ok=" + boolString(out.OK) + ", value=" + out.Value
		},
		DescribeState: func(state interface{}) string {
			return state.(string)
		},
	}
}

func partitionOperationsByKey(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := make(map[string][]porcupine.Operation)
	for _, op := range history {
		input, ok := op.Input.(linearizabilityInput)
		if !ok {
			continue
		}
		byKey[input.Key] = append(byKey[input.Key], op)
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	partitions := make([][]porcupine.Operation, 0, len(keys))
	for _, key := range keys {
		partitions = append(partitions, byKey[key])
	}
	if len(partitions) == 0 {
		return [][]porcupine.Operation{history}
	}
	return partitions
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
