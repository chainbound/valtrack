package discovery

type DiscoveryEvent struct{}

type DiscoveryService interface {
	// Start starts the discovery service and returns a channel that will notify of any new
	// discovery events.
	Start() (<-chan DiscoveryEvent, error)
	// Stop stops the discovery service.
	Stop() error
}
