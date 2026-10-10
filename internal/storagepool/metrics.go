package storagepool

// ServingMetrics uses only local state; no metadata/network IO and no per-path
// labels. It is exposed solely by existing authenticated storage status routes.
func (p *Pool) ServingMetrics() map[string]any {
	s := p.fetch
	s.mu.Lock()
	spools, files, flights, manifests := s.spoolBytes, s.spoolFiles, len(s.flights), s.manifestBytes
	s.mu.Unlock()
	p.cache.mu.Lock()
	retiredBytes := int64(0)
	for _, e := range p.cache.retired {
		retiredBytes += e.allocated
	}
	retained := p.cache.allocated
	p.cache.mu.Unlock()
	result := map[string]any{"queueFull": s.metrics.QueueFull.Load(), "waitTimeout": s.metrics.WaitTimeout.Load(), "diskAdmissionFailure": s.metrics.DiskDenied.Load(), "cacheHits": s.metrics.Hits.Load(), "cacheMisses": s.metrics.Misses.Load(), "sharedDownloads": s.metrics.Shared.Load(), "retries": s.metrics.Retries.Load(), "activeFetches": len(p.downloads), "waitingReaders": len(s.callers), "flights": flights, "temporaryBytes": spools, "temporaryFiles": files, "cacheBytes": retained, "retiredBytes": retiredBytes, "manifestBytes": manifests}
	if p.store != nil {
		result["metadata"] = p.store.ServingMetrics()
	}
	return result
}
