package cluster

import "time"

// Positive enrollment fixtures tolerate temporary authority loss while a
// generation, configuration or async lease catches up. Keep enroll itself
// single-shot: refusal tests must still observe a denied request directly.
func (h *churnCluster) awaitEnrollment(n *churnNode) {
	h.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := h.enroll(n)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("enrollment of %s did not converge: %v", n.config.Local.ID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
